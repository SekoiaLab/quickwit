// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use tantivy::schema::{Field, FieldType, Schema as TantivySchema};

use super::json_number_wildcard::{JsonNumberWildcard, can_match_numbers};
use super::tantivy_query_ast::TantivyBoolQuery;
use super::{BuildTantivyAst, QueryAst};
use crate::query_ast::{AutomatonQuery, BuildTantivyAstContext, JsonPathPrefix, TantivyQueryAst};
use crate::tokenizers::TokenizerManager;
use crate::{InvalidQuery, JsonPath, find_field_or_hit_dynamic};

/// A Wildcard query allows to match 'bond' with a query like 'b*d'.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct WildcardQuery {
    pub field: String,
    pub value: String,
    /// Support missing fields
    pub lenient: bool,
    pub case_insensitive: bool,
}

impl From<WildcardQuery> for QueryAst {
    fn from(wildcard_query: WildcardQuery) -> Self {
        Self::Wildcard(wildcard_query)
    }
}

pub(super) fn parse_wildcard_query(mut query: &str) -> Vec<SubQuery> {
    let mut res = Vec::new();
    while let Some(pos) = query.find(['*', '?', '\\']) {
        if pos > 0 {
            res.push(SubQuery::Text(query[..pos].to_string()));
        }
        let chr = &query[pos..pos + 1];
        query = &query[pos + 1..];
        match chr {
            "*" => res.push(SubQuery::Wildcard),
            "?" => res.push(SubQuery::QuestionMark),
            "\\" => {
                if let Some(chr) = query.chars().next() {
                    res.push(SubQuery::Text(chr.to_string()));
                    query = &query[chr.len_utf8()..];
                } else {
                    // escaping at the end is invalid, handle it as if that escape sequence wasn't
                    // present
                    break;
                }
            }
            _ => unreachable!("find shouldn't return non-matching position"),
        }
    }
    if !query.is_empty() {
        res.push(SubQuery::Text(query.to_string()));
    }
    res
}

pub(super) enum SubQuery {
    Text(String),
    Wildcard,
    QuestionMark,
}

fn sub_query_parts_to_regex(
    sub_query_parts: Vec<SubQuery>,
    tokenizer_name: &str,
    tokenizer_manager: &TokenizerManager,
) -> anyhow::Result<String> {
    let mut normalizer = tokenizer_manager
        .get_normalizer(tokenizer_name)
        .with_context(|| format!("no tokenizer named `{tokenizer_name}` is registered"))?;

    sub_query_parts
        .into_iter()
        .map(|part| match part {
            SubQuery::Text(text) => {
                let mut token_stream = normalizer.token_stream(&text);
                let expected_token = token_stream
                    .next()
                    .context("normalizer generated no content")?
                    .text
                    .clone();
                if let Some(_unexpected_token) = token_stream.next() {
                    bail!("normalizer generated multiple tokens")
                }
                Ok(Cow::Owned(regex::escape(&expected_token)))
            }
            SubQuery::Wildcard => Ok(Cow::Borrowed(".*")),
            SubQuery::QuestionMark => Ok(Cow::Borrowed(".")),
        })
        .collect::<Result<String, _>>()
}

impl WildcardQuery {
    pub fn to_regex(
        &self,
        schema: &TantivySchema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<(Field, Option<JsonPath>, String), InvalidQuery> {
        let Some((field, field_entry, json_path)) = find_field_or_hit_dynamic(&self.field, schema)
        else {
            return Err(InvalidQuery::FieldDoesNotExist {
                full_path: self.field.clone(),
            });
        };
        let field_type = field_entry.field_type();

        let sub_query_parts = parse_wildcard_query(&self.value);

        match field_type {
            FieldType::Str(text_options) => {
                let text_field_indexing = text_options.get_indexing_options().ok_or_else(|| {
                    InvalidQuery::SchemaError(format!(
                        "field {} is not full-text searchable",
                        field_entry.name()
                    ))
                })?;
                let tokenizer_name = text_field_indexing.tokenizer();
                let regex =
                    sub_query_parts_to_regex(sub_query_parts, tokenizer_name, tokenizer_manager)?;
                let regex =
                    if self.case_insensitive && self.value.chars().any(|c| c.is_alphabetic()) {
                        format!("(?i){}", regex)
                    } else {
                        regex
                    };

                Ok((field, None, regex))
            }
            FieldType::JsonObject(json_options) => {
                let text_field_indexing =
                    json_options.get_text_indexing_options().ok_or_else(|| {
                        InvalidQuery::SchemaError(format!(
                            "field {} is not full-text searchable",
                            field_entry.name()
                        ))
                    })?;
                let tokenizer_name = text_field_indexing.tokenizer();
                let regex =
                    sub_query_parts_to_regex(sub_query_parts, tokenizer_name, tokenizer_manager)?;
                let regex =
                    if self.case_insensitive && self.value.chars().any(|c| c.is_alphabetic()) {
                        format!("(?i){}", regex)
                    } else {
                        regex
                    };

                let byte_path_prefix =
                    JsonPath::from_json_path(json_path, json_options.is_expand_dots_enabled());

                Ok((field, Some(byte_path_prefix), regex))
            }
            _ => Err(InvalidQuery::SchemaError(
                "trying to run a Wildcard query on a non-text field".to_string(),
            )),
        }
    }

    /// Returns the automaton matching the numbers of a JSON field whose decimal text matches the
    /// wildcard, if any can: the regex of `to_regex` only matches the text values of the field.
    ///
    /// `json_path` is the one `to_regex` returned. The returned JSON path is the prefix shared by
    /// the terms of every type of the field, which the automaton follows.
    pub fn to_json_number_wildcard(
        &self,
        json_path: Option<&JsonPath>,
    ) -> Option<(JsonPath, JsonNumberWildcard)> {
        let json_path = json_path?;
        let number_wildcard =
            JsonNumberWildcard::from_sub_queries(&parse_wildcard_query(&self.value))?;
        Some((json_path.without_type_code(), number_wildcard))
    }

    /// Returns whether a number can match the wildcard, whatever the field: on a JSON field, such
    /// a wildcard runs a second automaton, on the numbers of the field.
    pub fn can_match_numbers(&self) -> bool {
        can_match_numbers(&parse_wildcard_query(&self.value))
    }
}

impl BuildTantivyAst for WildcardQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let (field, path, regex) = match self.to_regex(context.schema, context.tokenizer_manager) {
            Ok(res) => res,
            Err(InvalidQuery::FieldDoesNotExist { .. }) if self.lenient => {
                return Ok(TantivyQueryAst::match_none());
            }
            Err(e) => return Err(e),
        };
        let number_wildcard = self.to_json_number_wildcard(path.as_ref());
        let regex =
            tantivy_fst::Regex::new(&regex).context("failed to parse regex built from wildcard")?;
        let regex_automaton_with_path = JsonPathPrefix {
            prefix: path.unwrap_or_default(),
            automaton: regex.into(),
        };
        let regex_query_with_path: TantivyQueryAst = AutomatonQuery {
            field,
            automaton: Arc::new(regex_automaton_with_path),
        }
        .into();
        let Some((number_path, number_wildcard)) = number_wildcard else {
            return Ok(regex_query_with_path);
        };
        let number_query_with_path = AutomatonQuery {
            field,
            automaton: Arc::new(JsonPathPrefix {
                prefix: number_path,
                automaton: Arc::new(number_wildcard.to_automaton()?),
            }),
        };
        Ok(TantivyBoolQuery {
            should: vec![regex_query_with_path, number_query_with_path.into()],
            ..Default::default()
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use tantivy::schema::{TEXT, TextFieldIndexing, TextOptions};

    use super::*;
    use crate::create_default_quickwit_tokenizer_manager;

    fn single_text_field_schema(field_name: &str, tokenizer: &str) -> TantivySchema {
        let mut schema_builder = TantivySchema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
        schema_builder.add_text_field(field_name, text_options);
        schema_builder.build()
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
            lenient: false,
            case_insensitive: false,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
            assert!(path.is_none());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert!(path.is_none());
        }
    }

    #[test]
    fn test_wildcard_query_to_regex_on_escaped_text() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch\\?a.nOrMal Tokenizer would\\*cut".to_string(),
            lenient: false,
            case_insensitive: false,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch\\?a\\.nOrMal Tokenizer would\\*cut");
            assert!(path.is_none());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "mystring wh1ch\\?a\\.normal tokenizer would\\*cut");
            assert!(path.is_none());
        }
    }

    #[test]
    fn test_wildcard_query_to_regex_on_json() {
        let query = WildcardQuery {
            // this volontarily contains uppercase and regex-unsafe char to make sure we properly
            // keep the case, but sanitize special chars
            field: "json_field.Inner.Fie*ld".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
            lenient: false,
            case_insensitive: false,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_json_field("json_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
            assert_eq!(path.unwrap().0, "Inner\u{1}Fie*ld\0s".as_bytes());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_json_field("json_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert_eq!(path.unwrap().0, "Inner\u{1}Fie*ld\0s".as_bytes());
        }
    }

    #[test]
    fn test_extract_regex_wildcard_missing_field() {
        let query = WildcardQuery {
            field: "my_missing_field".to_string(),
            value: "My query value*".to_string(),
            lenient: false,
            case_insensitive: false,
        };
        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        let schema = single_text_field_schema("my_field", "whitespace");
        let err = query.to_regex(&schema, &tokenizer_manager).unwrap_err();
        let InvalidQuery::FieldDoesNotExist {
            full_path: missing_field_full_path,
        } = err
        else {
            panic!("unexpected error: {err:?}");
        };
        assert_eq!(missing_field_full_path, "my_missing_field");
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text_case_insensitive() {
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "MyString Wh1ch?a.nOrMal Tokenizer would*cut".to_string(),
            lenient: false,
            case_insensitive: true,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "(?i)MyString Wh1ch.a\\.nOrMal Tokenizer would.*cut");
            assert!(path.is_none());
        }

        for tokenizer in [
            "raw_lowercase",
            "lowercase",
            "default",
            "chinese_compatible",
            "source_code_default",
            "source_code_with_hex",
        ] {
            let mut schema_builder = TantivySchema::builder();
            let text_options = TextOptions::default()
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer(tokenizer));
            schema_builder.add_text_field("text_field", text_options);
            let schema = schema_builder.build();

            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "(?i)mystring wh1ch.a\\.normal tokenizer would.*cut");
            assert!(path.is_none());
        }
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text_case_insensitive_no_letters() {
        // When the pattern contains no letters, case_insensitive should have no effect
        // (no (?i) prefix added).
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "1234*5678".to_string(),
            lenient: false,
            case_insensitive: true,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        for tokenizer in ["raw", "whitespace"] {
            let schema = single_text_field_schema("text_field", tokenizer);
            let (_field, path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            assert_eq!(regex, "1234.*5678");
            assert!(path.is_none());
        }
    }

    #[test]
    fn test_wildcard_query_to_regex_on_text_case_insensitive_mixed() {
        // A pattern with both letters and digits still gets (?i).
        let query = WildcardQuery {
            field: "text_field".to_string(),
            value: "abc123*".to_string(),
            lenient: false,
            case_insensitive: true,
        };

        let tokenizer_manager = create_default_quickwit_tokenizer_manager();
        let schema = single_text_field_schema("text_field", "raw");
        let (_field, _path, regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
        assert_eq!(regex, "(?i)abc123.*");
    }

    fn json_field_schema() -> TantivySchema {
        let mut schema_builder = TantivySchema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw"));
        schema_builder.add_json_field("json_field", text_options);
        schema_builder.add_text_field("text_field", TEXT);
        schema_builder.build()
    }

    fn wildcard_query(field: &str, value: &str) -> WildcardQuery {
        WildcardQuery {
            field: field.to_string(),
            value: value.to_string(),
            lenient: false,
            case_insensitive: false,
        }
    }

    #[test]
    fn test_wildcard_query_to_json_number_wildcard() {
        let schema = json_field_schema();
        let tokenizer_manager = create_default_quickwit_tokenizer_manager();

        let query = wildcard_query("json_field.status", "2??");
        assert!(query.can_match_numbers());
        let (_field, path, _regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
        let (number_path, _) = query.to_json_number_wildcard(path.as_ref()).unwrap();
        assert_eq!(number_path.0, "status\0".as_bytes());

        // No number has a letter in its text.
        let query = wildcard_query("json_field.status", "2?a");
        assert!(!query.can_match_numbers());
        let (_field, path, _regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
        assert!(query.to_json_number_wildcard(path.as_ref()).is_none());

        // A text field has no number.
        let query = wildcard_query("text_field", "2??");
        let (_field, path, _regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
        assert!(query.to_json_number_wildcard(path.as_ref()).is_none());
    }

    #[test]
    fn test_wildcard_query_matches_numbers_on_json_fields() {
        let schema = json_field_schema();
        let context = BuildTantivyAstContext::for_test(&schema);

        let TantivyQueryAst::Bool(bool_query) = wildcard_query("json_field.status", "2??")
            .build_tantivy_ast_call(&context)
            .unwrap()
        else {
            panic!("expected a bool query on the text and the number terms");
        };
        assert_eq!(bool_query.should.len(), 2);
        assert!(bool_query.must.is_empty());
        assert!(bool_query.filter.is_empty());

        for (field, value) in [("json_field.status", "2?a"), ("text_field", "2??")] {
            let tantivy_ast = wildcard_query(field, value)
                .build_tantivy_ast_call(&context)
                .unwrap();
            assert!(
                matches!(tantivy_ast, TantivyQueryAst::Leaf(_)),
                "{field}: {value}"
            );
        }
    }
}
