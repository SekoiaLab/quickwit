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

//! Matching a wildcard pattern against the numbers of a JSON field.
//!
//! Tantivy indexes the numbers of a JSON field as numeric terms: the JSON path, a type code
//! (`i64`, `u64` or `f64`) and the 8 big-endian bytes of the value mapped to a sortable `u64`.
//! A regex on the text terms of the path never sees them. [`JsonNumberWildcardAutomaton`] walks
//! these terms instead: it decodes each number, and matches its decimal text against the
//! wildcards.

use anyhow::Context;
use tantivy::schema::Type;
use tantivy::{u64_to_f64, u64_to_i64};
use tantivy_fst::{Automaton, DisjunctionRegex, Regex};

use super::wildcard_query::SubQuery;

/// The most digits an integer of the index can have (`u64::MAX` has 20).
const MAX_INTEGER_DIGITS: u32 = 20;

/// An inclusive interval of integers.
type IntInterval = (i128, i128);

/// A wildcard pattern, to match against the numbers of a JSON field.
///
/// This is the specification of a [`JsonNumberWildcardAutomaton`]: unlike the automaton, it can be
/// hashed and compared, to deduplicate warmups.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JsonNumberWildcard {
    /// The regex of the wildcard, matched against the decimal text of a number.
    ///
    /// It is built from the pattern as written: unlike text, numbers aren't tokenized.
    regex: String,
    /// The only integers whose text can match the pattern, sorted. `None` when they can't be
    /// bounded (the pattern starts with a wildcard).
    int_intervals: Option<Vec<IntInterval>>,
}

/// One character of a wildcard pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternItem {
    Char(char),
    /// `?`: any character.
    Any,
    /// `*`: any sequence of characters.
    Star,
}

/// Returns the characters of the pattern, or `None` if no number can match it: it is empty, or has
/// a character that is neither a wildcard nor part of a number.
fn number_pattern_items(sub_queries: &[SubQuery]) -> Option<Vec<PatternItem>> {
    let mut items = Vec::new();
    for sub_query in sub_queries {
        match sub_query {
            SubQuery::Text(text) => {
                if !text
                    .chars()
                    .all(|c| c.is_ascii_digit() || c == '.' || c == '-')
                {
                    return None;
                }
                items.extend(text.chars().map(PatternItem::Char));
            }
            SubQuery::Wildcard => items.push(PatternItem::Star),
            SubQuery::QuestionMark => items.push(PatternItem::Any),
        }
    }
    // An empty pattern only matches empty text.
    if items.is_empty() {
        return None;
    }
    Some(items)
}

/// Returns whether a number can match the wildcard pattern.
pub(super) fn can_match_numbers(sub_queries: &[SubQuery]) -> bool {
    number_pattern_items(sub_queries).is_some()
}

impl JsonNumberWildcard {
    /// Returns `None` if no number can match the pattern, see [`can_match_numbers`].
    pub(super) fn from_sub_queries(sub_queries: &[SubQuery]) -> Option<Self> {
        let items = number_pattern_items(sub_queries)?;
        let regex = items
            .iter()
            .map(|item| match item {
                PatternItem::Char(c) => regex::escape(c.encode_utf8(&mut [0; 4])),
                PatternItem::Any => ".".to_string(),
                PatternItem::Star => ".*".to_string(),
            })
            .collect();
        Some(JsonNumberWildcard {
            regex,
            int_intervals: int_intervals(&items),
        })
    }

    pub fn to_automaton(&self) -> anyhow::Result<JsonNumberWildcardAutomaton> {
        JsonNumberWildcardAutomaton::from_wildcards(std::slice::from_ref(self))
    }
}

/// Returns the intervals holding every integer whose decimal text can match the pattern, so the
/// automaton can skip the others without decoding them. The intervals may hold integers that don't
/// match: the automaton matches the text of every integer it doesn't skip.
fn int_intervals(items: &[PatternItem]) -> Option<Vec<IntInterval>> {
    // A leading wildcard can be followed by any digits: no interval to restrict to.
    if !matches!(items.first(), Some(PatternItem::Char(_))) {
        return None;
    }
    // The decimal text of an integer has no `.`, and a `-` only as its first character.
    let no_integer_matches = items.iter().enumerate().any(|(position, item)| {
        *item == PatternItem::Char('.') || (position > 0 && *item == PatternItem::Char('-'))
    });
    if no_integer_matches {
        return Some(Vec::new());
    }
    let negative = items[0] == PatternItem::Char('-');
    let unsigned_items = &items[usize::from(negative)..];
    let num_prefix_digits = unsigned_items
        .iter()
        .take_while(|item| matches!(item, PatternItem::Char(_)))
        .count();
    let prefix_digits: String = unsigned_items[..num_prefix_digits]
        .iter()
        .filter_map(|item| match item {
            PatternItem::Char(digit) => Some(*digit),
            _ => None,
        })
        .collect();
    let rest = &unsigned_items[num_prefix_digits..];
    let min_num_extra_digits = rest
        .iter()
        .filter(|item| **item != PatternItem::Star)
        .count() as u32;
    let max_num_extra_digits = if rest.contains(&PatternItem::Star) {
        MAX_INTEGER_DIGITS
    } else {
        min_num_extra_digits
    };
    // The largest absolute value of an integer of the index with this sign.
    let max_magnitude: i128 = if negative {
        -(i64::MIN as i128)
    } else {
        u64::MAX as i128
    };
    let to_signed_interval = |low_magnitude: i128, high_magnitude: i128| -> IntInterval {
        let high_magnitude = high_magnitude.min(max_magnitude);
        if negative {
            (-high_magnitude, -low_magnitude)
        } else {
            (low_magnitude, high_magnitude)
        }
    };

    if prefix_digits.is_empty() {
        // `-` followed by a wildcard: any negative integer.
        return Some(vec![to_signed_interval(1, max_magnitude)]);
    }
    if prefix_digits.starts_with('0') {
        // The only integer whose text starts with `0` is 0 itself.
        let zero_matches = prefix_digits == "0" && min_num_extra_digits == 0 && !negative;
        return Some(if zero_matches {
            vec![(0, 0)]
        } else {
            Vec::new()
        });
    }
    // Too many digits for an `i128`: too many for an integer of the index too.
    let Ok(prefix) = prefix_digits.parse::<i128>() else {
        return Some(Vec::new());
    };
    let mut intervals = Vec::new();
    for num_extra_digits in min_num_extra_digits..=max_num_extra_digits {
        // The integers made of the prefix followed by `num_extra_digits` digits.
        let Some(scale) = 10i128.checked_pow(num_extra_digits) else {
            break;
        };
        let Some(low_magnitude) = prefix.checked_mul(scale) else {
            break;
        };
        if low_magnitude > max_magnitude {
            break;
        }
        let high_magnitude = match (prefix + 1).checked_mul(scale) {
            Some(end) => end - 1,
            None => i128::MAX,
        };
        intervals.push(to_signed_interval(low_magnitude, high_magnitude));
    }
    intervals.sort_unstable();
    Some(intervals)
}

/// An automaton matching the numeric terms of a JSON path whose decimal text matches any of
/// several wildcards.
///
/// It is meant to be used after the JSON path prefix (`path\0`) through
/// [`super::JsonPathPrefix`]: it reads a type code and the 8 bytes of the value.
pub struct JsonNumberWildcardAutomaton {
    regex: DisjunctionRegex,
    int_intervals: Option<Vec<IntInterval>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonNumberWildcardState {
    /// Expects the type code.
    Type,
    /// Reads the bytes of the value.
    Value {
        value_type: Type,
        num_bytes: u8,
        bytes: u64,
    },
    /// Read a whole number whose text matches.
    Match,
    /// Can't match any longer.
    Dead,
}

/// Returns whether the automaton matches the whole text.
fn text_matches<A: Automaton>(automaton: &A, text: &str) -> bool {
    let mut state = automaton.start();
    for byte in text.bytes() {
        state = automaton.accept(&state, byte);
        if !automaton.can_match(&state) {
            return false;
        }
    }
    automaton.is_match(&state)
}

impl JsonNumberWildcardAutomaton {
    /// Builds the automaton matching the numbers that match any of the wildcards, to walk the
    /// numeric terms of a path once for all of them.
    pub fn from_wildcards(wildcards: &[JsonNumberWildcard]) -> anyhow::Result<Self> {
        let patterns: Vec<&str> = wildcards
            .iter()
            .map(|wildcard| wildcard.regex.as_str())
            .collect();
        let regex = Regex::from_patterns(&patterns)
            .with_context(|| format!("failed to parse regexes {patterns:?}"))?;
        // The integers that can match any of the wildcards; unbounded if one of them is.
        let int_intervals = wildcards
            .iter()
            .map(|wildcard| wildcard.int_intervals.clone())
            .collect::<Option<Vec<Vec<IntInterval>>>>()
            .map(|intervals| intervals.concat());
        Ok(JsonNumberWildcardAutomaton {
            regex,
            int_intervals,
        })
    }

    fn matches(&self, value_type: Type, bytes: u64) -> bool {
        let text = match value_type {
            Type::I64 => u64_to_i64(bytes).to_string(),
            Type::U64 => bytes.to_string(),
            Type::F64 => u64_to_f64(bytes).to_string(),
            _ => return false,
        };
        match &self.regex {
            DisjunctionRegex::Single(regex) => text_matches(regex, &text),
            DisjunctionRegex::Multi(regexes) => text_matches(regexes, &text),
        }
    }

    /// Returns whether the values starting with the `num_bytes` bytes read so far can match.
    fn may_match(&self, value_type: Type, num_bytes: u8, bytes: u64) -> bool {
        let Some(int_intervals) = &self.int_intervals else {
            return true;
        };
        let decode = |encoded: u128| -> i128 {
            match value_type {
                Type::I64 => u64_to_i64(encoded as u64) as i128,
                _ => encoded as i128,
            }
        };
        let num_unknown_bits = 8 * (8 - u32::from(num_bytes));
        let lowest_encoded = u128::from(bytes) << num_unknown_bits;
        let highest_encoded = lowest_encoded + (1u128 << num_unknown_bits) - 1;
        // The sortable encoding is monotonic: the values read so far span the interval between the
        // lowest and the highest encoded value.
        let (lowest, highest) = (decode(lowest_encoded), decode(highest_encoded));
        int_intervals
            .iter()
            .any(|&(low, high)| low <= highest && lowest <= high)
    }
}

impl Automaton for JsonNumberWildcardAutomaton {
    type State = JsonNumberWildcardState;

    fn start(&self) -> Self::State {
        JsonNumberWildcardState::Type
    }

    fn is_match(&self, state: &Self::State) -> bool {
        *state == JsonNumberWildcardState::Match
    }

    fn can_match(&self, state: &Self::State) -> bool {
        match *state {
            JsonNumberWildcardState::Type | JsonNumberWildcardState::Match => true,
            JsonNumberWildcardState::Dead => false,
            // Floats are only indexed as `f64` when they aren't integers: they are rare, and their
            // text can't be bounded by integer intervals.
            JsonNumberWildcardState::Value {
                value_type: Type::F64,
                ..
            } => true,
            JsonNumberWildcardState::Value {
                value_type,
                num_bytes,
                bytes,
            } => self.may_match(value_type, num_bytes, bytes),
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        match *state {
            JsonNumberWildcardState::Type => match Type::from_code(byte) {
                Some(value_type @ (Type::I64 | Type::U64 | Type::F64)) => {
                    JsonNumberWildcardState::Value {
                        value_type,
                        num_bytes: 0,
                        bytes: 0,
                    }
                }
                _ => JsonNumberWildcardState::Dead,
            },
            JsonNumberWildcardState::Value {
                value_type,
                num_bytes,
                bytes,
            } => {
                let bytes = (bytes << 8) | u64::from(byte);
                let num_bytes = num_bytes + 1;
                if num_bytes < 8 {
                    JsonNumberWildcardState::Value {
                        value_type,
                        num_bytes,
                        bytes,
                    }
                } else if self.matches(value_type, bytes) {
                    JsonNumberWildcardState::Match
                } else {
                    JsonNumberWildcardState::Dead
                }
            }
            JsonNumberWildcardState::Match | JsonNumberWildcardState::Dead => {
                JsonNumberWildcardState::Dead
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::Term;
    use tantivy::fastfield::FastValue;
    use tantivy::schema::Field;
    use tantivy_fst::Automaton;

    use super::{JsonNumberWildcard, JsonNumberWildcardAutomaton, JsonNumberWildcardState};
    use crate::query_ast::wildcard_query::parse_wildcard_query;

    fn json_number_wildcard(pattern: &str) -> Option<JsonNumberWildcard> {
        JsonNumberWildcard::from_sub_queries(&parse_wildcard_query(pattern))
    }

    fn intervals(pattern: &str) -> Option<Vec<(i128, i128)>> {
        json_number_wildcard(pattern).unwrap().int_intervals
    }

    /// The bytes the automaton reads for a term: what follows `path\0`.
    fn after_json_path(term: &Term) -> Vec<u8> {
        let bytes = term.serialized_value_bytes();
        let end_of_path = bytes.iter().position(|byte| *byte == 0).unwrap();
        bytes[end_of_path + 1..].to_vec()
    }

    fn number_term_bytes<T: FastValue>(value: T) -> Vec<u8> {
        let mut term = Term::from_field_json_path(Field::from_field_id(0), "status", false);
        term.append_type_and_fast_value(value);
        after_json_path(&term)
    }

    fn str_term_bytes(text: &str) -> Vec<u8> {
        let mut term = Term::from_field_json_path(Field::from_field_id(0), "status", false);
        term.append_type_and_str(text);
        after_json_path(&term)
    }

    /// Runs the automaton over the bytes, stopping as soon as it can't match, like a term
    /// dictionary stream. Returns the final state, and how many bytes it read.
    fn run(pattern: &str, bytes: &[u8]) -> (JsonNumberWildcardState, usize) {
        let automaton = json_number_wildcard(pattern)
            .unwrap()
            .to_automaton()
            .unwrap();
        run_automaton(&automaton, bytes)
    }

    fn run_automaton(
        automaton: &JsonNumberWildcardAutomaton,
        bytes: &[u8],
    ) -> (JsonNumberWildcardState, usize) {
        let mut state = automaton.start();
        for (num_bytes_read, byte) in bytes.iter().enumerate() {
            if !automaton.can_match(&state) {
                return (state, num_bytes_read);
            }
            state = automaton.accept(&state, *byte);
        }
        (state, bytes.len())
    }

    fn matches(pattern: &str, bytes: &[u8]) -> bool {
        run(pattern, bytes).0 == JsonNumberWildcardState::Match
    }

    #[test]
    fn test_json_number_wildcard_requires_number_characters() {
        assert!(json_number_wildcard("2*").is_some());
        assert!(json_number_wildcard("-1.5?").is_some());
        assert!(json_number_wildcard("*").is_some());
        assert!(json_number_wildcard("").is_none());
        assert!(json_number_wildcard("a*").is_none());
        assert!(json_number_wildcard("2*b").is_none());
        // An escaped wildcard is a literal character.
        assert!(json_number_wildcard("2\\*").is_none());
    }

    #[test]
    fn test_int_intervals() {
        let pow10 = |exponent: u32| 10i128.pow(exponent);
        assert_eq!(intervals("2??"), Some(vec![(200, 299)]));
        // Up to the u64 maximum, 18446744073709551615.
        let mut expected: Vec<(i128, i128)> =
            (0..19).map(|n| (2 * pow10(n), 3 * pow10(n) - 1)).collect();
        assert_eq!(intervals("2*"), Some(expected.clone()));
        expected.remove(0);
        assert_eq!(intervals("2?*"), Some(expected));
        assert_eq!(
            intervals("1844674407370955161*"),
            Some(vec![
                (1844674407370955161, 1844674407370955161),
                (18446744073709551610, u64::MAX as i128),
            ])
        );
        assert_eq!(intervals("4?4"), Some(vec![(400, 499)]));
        assert_eq!(intervals("0*"), Some(vec![(0, 0)]));
        assert_eq!(intervals("0?"), Some(vec![]));
        assert_eq!(intervals("-0*"), Some(vec![]));
        assert_eq!(intervals("-1?"), Some(vec![(-19, -10)]));
        // Down to the i64 minimum, -9223372036854775808.
        assert_eq!(
            intervals("-922337203685477580*"),
            Some(vec![
                (i64::MIN as i128, -9223372036854775800),
                (-922337203685477580, -922337203685477580),
            ])
        );
        assert_eq!(intervals("-*"), Some(vec![(i64::MIN as i128, -1)]));
        assert_eq!(intervals("2.5*"), Some(vec![]));
        assert_eq!(intervals("2-*"), Some(vec![]));
        assert_eq!(intervals("123456789012345678901*"), Some(vec![]));
        assert_eq!(intervals("*04"), None);
        assert_eq!(intervals("?0"), None);
    }

    #[test]
    fn test_json_number_wildcard_automaton_matches_number_text() {
        assert!(matches("2??", &number_term_bytes(200i64)));
        assert!(matches("2??", &number_term_bytes(299i64)));
        assert!(!matches("2??", &number_term_bytes(2i64)));
        assert!(!matches("2??", &number_term_bytes(2000i64)));
        assert!(!matches("2??", &number_term_bytes(200.5f64)));

        assert!(matches("2*", &number_term_bytes(2i64)));
        assert!(matches("2*", &number_term_bytes(250000i64)));
        assert!(matches("2*", &number_term_bytes(2.5f64)));
        assert!(matches("2.5", &number_term_bytes(2.5f64)));
        assert!(!matches("2*", &number_term_bytes(12i64)));

        assert!(matches("*04", &number_term_bytes(404i64)));
        assert!(matches("*04", &number_term_bytes(1004i64)));
        assert!(!matches("*04", &number_term_bytes(405i64)));
        assert!(matches("4?4", &number_term_bytes(404i64)));
        assert!(matches("-1?", &number_term_bytes(-12i64)));
        assert!(!matches("-1?", &number_term_bytes(12i64)));
        assert!(matches("0*", &number_term_bytes(0i64)));
        assert!(matches("*", &number_term_bytes(-7i64)));

        // Values above `i64::MAX` are indexed as `u64`.
        assert!(matches(
            "1844674407370955161?",
            &number_term_bytes(u64::MAX)
        ));
        assert!(!matches(
            "1844674407370955161?",
            &number_term_bytes(i64::MAX)
        ));
    }

    #[test]
    fn test_json_number_wildcard_automaton_ignores_other_types() {
        assert!(!matches("2*", &str_term_bytes("200")));
        assert!(!matches("*", &str_term_bytes("200")));
        assert!(!matches("*", &number_term_bytes(true)));
        // Nothing follows the 8 bytes of a number.
        let mut bytes = number_term_bytes(200i64);
        bytes.push(0);
        assert!(!matches("2??", &bytes));
    }

    #[test]
    fn test_json_number_wildcard_automaton_skips_integers_outside_the_intervals() {
        // The automaton reads the type code, then the 8 bytes of the value. Stopping before the
        // 9th byte means the term dictionary skips the value without decoding it.
        //
        // 1000 shares its first 6 bytes with the integers in [200, 299], not its 7th.
        let (_, num_bytes_read) = run("2??", &number_term_bytes(1000i64));
        assert_eq!(num_bytes_read, 8);
        // A negative integer differs from [200, 299] from its first byte on.
        let (_, num_bytes_read) = run("2??", &number_term_bytes(-200i64));
        assert_eq!(num_bytes_read, 2);
        let (_, num_bytes_read) = run("2??", &number_term_bytes(u64::MAX));
        assert_eq!(num_bytes_read, 2);
        // No integer has a `.` in its text.
        let (_, num_bytes_read) = run("2.5", &number_term_bytes(2i64));
        assert_eq!(num_bytes_read, 1);
        // No pruning on a leading wildcard.
        let (state, num_bytes_read) = run("*04", &number_term_bytes(1000i64));
        assert_eq!(state, JsonNumberWildcardState::Dead);
        assert_eq!(num_bytes_read, 9);
    }

    #[test]
    fn test_json_number_wildcard_automaton_matches_any_of_several_wildcards() {
        let automaton = |patterns: &[&str]| {
            let wildcards: Vec<JsonNumberWildcard> = patterns
                .iter()
                .map(|pattern| json_number_wildcard(pattern).unwrap())
                .collect();
            JsonNumberWildcardAutomaton::from_wildcards(&wildcards).unwrap()
        };
        let matches = |automaton: &JsonNumberWildcardAutomaton, bytes: &[u8]| {
            run_automaton(automaton, bytes).0 == JsonNumberWildcardState::Match
        };

        let status_codes = automaton(&["2??", "3??"]);
        assert!(matches(&status_codes, &number_term_bytes(204i64)));
        assert!(matches(&status_codes, &number_term_bytes(301i64)));
        assert!(!matches(&status_codes, &number_term_bytes(404i64)));
        // Both wildcards bound the integers: 1000 is skipped before its last byte.
        let (_, num_bytes_read) = run_automaton(&status_codes, &number_term_bytes(1000i64));
        assert_eq!(num_bytes_read, 8);

        // `*04` bounds no integer: nothing is skipped.
        let mixed = automaton(&["2??", "*04"]);
        assert!(matches(&mixed, &number_term_bytes(200i64)));
        assert!(matches(&mixed, &number_term_bytes(1004i64)));
        let (state, num_bytes_read) = run_automaton(&mixed, &number_term_bytes(1000i64));
        assert_eq!(state, JsonNumberWildcardState::Dead);
        assert_eq!(num_bytes_read, 9);
    }

    #[test]
    fn test_json_number_wildcard_ignores_the_tokenizer() {
        use tantivy::schema::{Schema, TextFieldIndexing, TextOptions};

        use crate::create_default_quickwit_tokenizer_manager;
        use crate::query_ast::WildcardQuery;

        // The default tokenizer drops `-` and `.` from text, not from numbers.
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_tokenizer("default"));
        schema_builder.add_json_field("json", text_options);
        let schema = schema_builder.build();
        let tokenizer_manager = create_default_quickwit_tokenizer_manager();

        for (pattern, number, expected) in [
            ("-1?", -12i64, true),
            ("-1?", 12i64, false),
            ("1-*", 15i64, false),
        ] {
            let query = WildcardQuery {
                field: "json.v".to_string(),
                value: pattern.to_string(),
                lenient: false,
                case_insensitive: false,
            };
            let (_field, path, _regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
            let (_, number_wildcard) = query.to_json_number_wildcard(path.as_ref()).unwrap();
            let automaton = number_wildcard.to_automaton().unwrap();
            let (state, _) = run_automaton(&automaton, &number_term_bytes(number));
            assert_eq!(
                state == JsonNumberWildcardState::Match,
                expected,
                "{pattern} on {number}"
            );
        }
        // A float isn't bounded by the integer intervals: the regex alone rejects it.
        let query = WildcardQuery {
            field: "json.v".to_string(),
            value: "1-*".to_string(),
            lenient: false,
            case_insensitive: false,
        };
        let (_field, path, _regex) = query.to_regex(&schema, &tokenizer_manager).unwrap();
        let (_, number_wildcard) = query.to_json_number_wildcard(path.as_ref()).unwrap();
        let automaton = number_wildcard.to_automaton().unwrap();
        let (state, _) = run_automaton(&automaton, &number_term_bytes(1.5f64));
        assert_eq!(state, JsonNumberWildcardState::Dead);
    }
}
