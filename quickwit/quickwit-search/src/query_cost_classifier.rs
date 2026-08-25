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

//! Heuristic classification of search queries by expected execution cost.
//!
//! The classification is used exclusively to label metrics (search thread
//! pool occupancy, search permits, ...), so that the resource usage of
//! expensive queries can be monitored separately from that of regular ones.
//! It must remain a cheap, pure function of the query AST: no I/O, no schema
//! access, and no query execution.

use quickwit_query::query_ast::{QueryAst, QueryAstVisitor, RegexQuery, WildcardQuery};

/// Minimum number of literal characters required before the first
/// wildcard/metacharacter of a regex or wildcard query pattern for that query
/// to be considered [`QueryCostClass::Regular`].
///
/// Patterns with a shorter literal prefix are considered
/// [`QueryCostClass::Costly`], since they can't rely on prefix pruning of the
/// term dictionary and may end up scanning a large fraction of it.
const MIN_LITERAL_PREFIX_LEN: usize = 3;

/// Coarse classification of the expected cost of executing a search query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryCostClass {
    /// The query is expected to run with predictable, moderate cost.
    Regular,
    /// The query contains patterns (e.g. unanchored regexes or wildcards)
    /// that can prevent efficient pruning of the term dictionary, and can be
    /// significantly more expensive to run than a [`QueryCostClass::Regular`]
    /// one.
    Costly,
}

impl QueryCostClass {
    /// Label value used when reporting this cost class in metrics.
    pub fn as_label(&self) -> &'static str {
        match self {
            QueryCostClass::Regular => "regular",
            QueryCostClass::Costly => "costly",
        }
    }
}

/// Converts to the wire representation, so the cost class computed once at
/// the root can be carried on [`quickwit_proto::search::SearchRequest`] down
/// to leaf/searcher nodes, instead of being reclassified there.
impl From<QueryCostClass> for quickwit_proto::search::QueryCostClass {
    fn from(cost_class: QueryCostClass) -> Self {
        match cost_class {
            QueryCostClass::Regular => quickwit_proto::search::QueryCostClass::Regular,
            QueryCostClass::Costly => quickwit_proto::search::QueryCostClass::Costly,
        }
    }
}

impl From<quickwit_proto::search::QueryCostClass> for QueryCostClass {
    fn from(cost_class: quickwit_proto::search::QueryCostClass) -> Self {
        match cost_class {
            quickwit_proto::search::QueryCostClass::Regular => QueryCostClass::Regular,
            quickwit_proto::search::QueryCostClass::Costly => QueryCostClass::Costly,
        }
    }
}

/// Classifies a query AST as [`QueryCostClass::Regular`] or
/// [`QueryCostClass::Costly`], based on simple heuristics.
///
/// This is a pure, cheap function (no I/O) meant to be called for every
/// query in order to attach a coarse-grained cost label to metrics.
pub fn classify(query_ast: &QueryAst) -> QueryCostClass {
    match CostClassifierVisitor.visit(query_ast) {
        Ok(()) => QueryCostClass::Regular,
        Err(Costly) => QueryCostClass::Costly,
    }
}

/// Sentinel error, used to stop the traversal as soon as a costly pattern is
/// found: the verdict can't be changed by the rest of the AST.
struct Costly;

struct CostClassifierVisitor;

impl<'a> QueryAstVisitor<'a> for CostClassifierVisitor {
    type Err = Costly;

    fn visit_regex(&mut self, regex_query: &'a RegexQuery) -> Result<(), Self::Err> {
        if has_min_literal_prefix(
            &regex_query.regex,
            MIN_LITERAL_PREFIX_LEN,
            PatternFlavor::Regex,
        ) {
            return Ok(());
        }
        Err(Costly)
    }

    fn visit_wildcard(&mut self, wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        if has_min_literal_prefix(
            &wildcard_query.value,
            MIN_LITERAL_PREFIX_LEN,
            PatternFlavor::Wildcard,
        ) {
            return Ok(());
        }
        Err(Costly)
    }
}

/// Pattern syntax being scanned, which determines what terminates a literal
/// prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternFlavor {
    /// Regex pattern, where `\` may introduce a character class or an
    /// assertion, and where `*`, `?` and `{0,n}` quantify the character that
    /// precedes them.
    Regex,
    /// Wildcard pattern, where `*` and `?` are the only wildcards and `\`
    /// always escapes the character that follows it.
    Wildcard,
}

impl PatternFlavor {
    /// Returns whether `c` terminates the literal prefix.
    fn is_metacharacter(self, c: char) -> bool {
        match self {
            PatternFlavor::Regex => matches!(
                c,
                '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$'
            ),
            PatternFlavor::Wildcard => matches!(c, '*' | '?'),
        }
    }

    /// Returns whether `\escaped` stands for the literal character `escaped`,
    /// rather than for a character class or an assertion.
    fn escape_is_literal(self, escaped: char) -> bool {
        match self {
            // `\d`, `\w`, `\s`, `\b`, `\p{..}`, ... match more than a single
            // known character, so they don't allow any prefix pruning. `\x41`
            // and friends do denote a literal, but we conservatively lump them
            // in with the character classes.
            PatternFlavor::Regex => !escaped.is_alphanumeric(),
            PatternFlavor::Wildcard => true,
        }
    }

    /// Returns whether `remainder` starts with a quantifier that makes the
    /// character preceding it optional, and which therefore doesn't extend the
    /// prunable prefix.
    ///
    /// Note that `+` and `{n,m}` with `n >= 1` are *not* such quantifiers:
    /// they require at least one occurrence of the character they apply to.
    fn starts_with_optional_quantifier(self, remainder: &str) -> bool {
        if self == PatternFlavor::Wildcard {
            return false;
        }
        let mut chars = remainder.chars();
        match chars.next() {
            Some('*' | '?') => true,
            Some('{') => {
                let min_repetition = chars.as_str().split([',', '}']).next().unwrap_or_default();
                matches!(min_repetition.parse::<u32>(), Ok(0))
            }
            _ => false,
        }
    }
}

/// Returns whether `pattern` has at least `min_len` literal characters at its
/// start, before the first metacharacter. A leading `^` regex anchor is
/// ignored, as it doesn't prevent prefix-based pruning of the term dictionary.
///
/// This only scans as far as necessary to answer the question: it never walks
/// past the first `min_len` literal characters (or the first metacharacter,
/// whichever comes first).
fn has_min_literal_prefix(pattern: &str, min_len: usize, flavor: PatternFlavor) -> bool {
    if min_len == 0 {
        return true;
    }
    let pattern = match flavor {
        PatternFlavor::Regex => pattern.strip_prefix('^').unwrap_or(pattern),
        PatternFlavor::Wildcard => pattern,
    };
    let mut prefix_len = 0;
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            let Some(escaped) = chars.next() else {
                // trailing, dangling escape: stop counting, this is likely not
                // a well formed pattern anyway.
                break;
            };
            if !flavor.escape_is_literal(escaped) {
                break;
            }
        } else if flavor.is_metacharacter(c) {
            break;
        }
        prefix_len += 1;
        if flavor.starts_with_optional_quantifier(chars.as_str()) {
            // the literal we just counted is optional, so it doesn't extend the
            // prunable prefix.
            prefix_len -= 1;
        }
        if prefix_len >= min_len {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use quickwit_query::query_ast::BoolQuery;

    use super::*;

    fn regex(pattern: &str) -> QueryAst {
        RegexQuery {
            field: "body".to_string(),
            regex: pattern.to_string(),
        }
        .into()
    }

    fn wildcard(pattern: &str) -> QueryAst {
        WildcardQuery {
            field: "body".to_string(),
            value: pattern.to_string(),
            lenient: false,
            case_insensitive: false,
        }
        .into()
    }

    #[test]
    fn test_match_all_is_regular() {
        assert_eq!(classify(&QueryAst::MatchAll), QueryCostClass::Regular);
    }

    #[test]
    fn test_plain_term_is_regular() {
        let ast: QueryAst = quickwit_query::query_ast::TermQuery {
            field: "body".to_string(),
            value: "hello".to_string(),
        }
        .into();
        assert_eq!(classify(&ast), QueryCostClass::Regular);
    }

    #[test]
    fn test_anchored_regex_with_long_prefix_is_regular() {
        assert_eq!(classify(&regex("^abcd.*")), QueryCostClass::Regular);
    }

    #[test]
    fn test_regex_with_short_literal_prefix_is_costly() {
        assert_eq!(classify(&regex("ab.*xyz")), QueryCostClass::Costly);
    }

    #[test]
    fn test_leading_wildcard_regex_is_costly() {
        assert_eq!(classify(&regex(".*xyz")), QueryCostClass::Costly);
    }

    #[test]
    fn test_escaped_metacharacter_counts_as_literal() {
        // "a\.b" matches the literal "a.b" and only then has a wildcard.
        assert_eq!(classify(&regex("a\\.b.*")), QueryCostClass::Regular);
    }

    #[test]
    fn test_escaped_character_class_is_costly() {
        // "\d\d\d" has no literal prefix at all: the escaped characters are
        // character classes, not literals.
        assert_eq!(classify(&regex("\\d\\d\\d456")), QueryCostClass::Costly);
        assert_eq!(classify(&regex("ab\\wxyz")), QueryCostClass::Costly);
    }

    #[test]
    fn test_regex_optional_quantifier_shortens_prefix() {
        // "abc*" is "ab" followed by any number of "c": the prunable prefix is
        // only 2 characters long.
        assert_eq!(classify(&regex("abc*")), QueryCostClass::Costly);
        assert_eq!(classify(&regex("abc?")), QueryCostClass::Costly);
        assert_eq!(classify(&regex("abc{0,5}")), QueryCostClass::Costly);
        // ... but one more literal character is enough.
        assert_eq!(classify(&regex("abcd*")), QueryCostClass::Regular);
    }

    #[test]
    fn test_regex_mandatory_quantifier_keeps_prefix() {
        // "abc+" and "abc{2,5}" both require at least one "c".
        assert_eq!(classify(&regex("abc+")), QueryCostClass::Regular);
        assert_eq!(classify(&regex("abc{2,5}")), QueryCostClass::Regular);
    }

    #[test]
    fn test_wildcard_query_with_leading_wildcard_is_costly() {
        assert_eq!(classify(&wildcard("*xyz")), QueryCostClass::Costly);
    }

    #[test]
    fn test_wildcard_query_with_long_prefix_is_regular() {
        assert_eq!(classify(&wildcard("abcdef*")), QueryCostClass::Regular);
    }

    #[test]
    fn test_wildcard_query_with_short_prefix_is_costly() {
        assert_eq!(classify(&wildcard("ab*")), QueryCostClass::Costly);
    }

    #[test]
    fn test_wildcard_query_escape_counts_as_literal() {
        // the ES `prefix` query escapes wildcards, so `a\dm` reaches us as
        // `a\\dm`: a 3 character literal prefix.
        assert_eq!(classify(&wildcard("a\\\\dm*")), QueryCostClass::Regular);
    }

    #[test]
    fn test_costly_query_nested_in_bool_query_is_costly() {
        let ast: QueryAst = BoolQuery {
            must: vec![regex(".*xyz")],
            ..Default::default()
        }
        .into();
        assert_eq!(classify(&ast), QueryCostClass::Costly);
    }

    #[test]
    fn test_regular_queries_nested_in_bool_query_are_regular() {
        let ast: QueryAst = BoolQuery {
            must: vec![regex("^abcd.*"), wildcard("abcdef*")],
            ..Default::default()
        }
        .into();
        assert_eq!(classify(&ast), QueryCostClass::Regular);
    }

    // The following tests exercise the boundary logic of the prefix scanner
    // directly, with arbitrary thresholds, since `classify` itself always
    // uses the fixed `MIN_LITERAL_PREFIX_LEN` constant.

    #[test]
    fn test_has_min_literal_prefix_threshold() {
        assert!(has_min_literal_prefix("ab.*xyz", 2, PatternFlavor::Regex));
        assert!(!has_min_literal_prefix("ab.*xyz", 3, PatternFlavor::Regex));
    }

    #[test]
    fn test_has_min_literal_prefix_stops_early() {
        // the scan must stop as soon as it reaches `min_len`, well before the
        // end of a long literal prefix.
        let long_prefix = "a".repeat(10_000);
        assert!(has_min_literal_prefix(
            &format!("{long_prefix}.*"),
            3,
            PatternFlavor::Regex
        ));
        assert!(has_min_literal_prefix(
            &format!("{long_prefix}*"),
            3,
            PatternFlavor::Wildcard
        ));
    }

    #[test]
    fn test_has_min_literal_prefix_zero_is_always_true() {
        assert!(has_min_literal_prefix(".*xyz", 0, PatternFlavor::Regex));
        assert!(has_min_literal_prefix("*xyz", 0, PatternFlavor::Wildcard));
    }

    #[test]
    fn test_has_min_literal_prefix_dangling_escape() {
        assert!(!has_min_literal_prefix("ab\\", 3, PatternFlavor::Regex));
        assert!(!has_min_literal_prefix("ab\\", 3, PatternFlavor::Wildcard));
    }
}
