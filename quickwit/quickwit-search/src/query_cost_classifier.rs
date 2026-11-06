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

//! Cheap checks of search queries by expected execution cost.
//!
//! They must remain cheap, pure functions of the query AST: no I/O, no schema
//! access, and no query execution.

use std::sync::LazyLock;

use quickwit_proto::search::SearchRequest;
use quickwit_query::query_ast::{QueryAst, QueryAstVisitor, RegexQuery, WildcardQuery};

use crate::SearchError;
use crate::cost::compute_query_complexity_factor;

/// Maximum number of regex and wildcard sub-queries allowed in a single search
/// request, checked by [`check_automaton_query_count`].
static MAX_AUTOMATON_QUERIES_PER_REQUEST: LazyLock<usize> = LazyLock::new(|| {
    quickwit_common::get_from_env("QW_MAX_AUTOMATON_QUERIES_PER_REQUEST", 200, false)
});

/// Returns the complexity factor of a search request, see [`compute_query_complexity_factor`].
///
/// A request that fails to parse gets the factor of the simplest query: it is rejected with a
/// proper error further down the line, and this is only about scheduling and logging.
pub fn query_complexity_factor_or_default(search_request: &SearchRequest) -> f32 {
    compute_query_complexity_factor(search_request).unwrap_or(1.0)
}

/// Rejects queries that contain more regex or wildcard sub-queries than
/// allowed by `QW_MAX_AUTOMATON_QUERIES_PER_REQUEST` (default 200).
///
/// Each regex or wildcard leaf compiles to a regex automaton per split and per
/// targeted field. A request with hundreds of them, fanned out over hundreds of
/// indexes, can spend minutes of CPU in planning/validation alone, so we bound
/// their number upfront.
///
/// This must be called on the *resolved* query AST: user input queries can
/// expand into regex/wildcard leaves during resolution.
pub fn check_automaton_query_count(query_ast: &QueryAst) -> Result<(), SearchError> {
    let max_automaton_queries = *MAX_AUTOMATON_QUERIES_PER_REQUEST;
    check_automaton_query_count_with_limit(query_ast, max_automaton_queries).map_err(
        |TooManyAutomatonQueries| {
            SearchError::InvalidQuery(format!(
                "query contains more than {max_automaton_queries} regex or wildcard sub-queries \
                 (limit configurable with the `QW_MAX_AUTOMATON_QUERIES_PER_REQUEST` env variable)"
            ))
        },
    )
}

fn check_automaton_query_count_with_limit(
    query_ast: &QueryAst,
    max_automaton_queries: usize,
) -> Result<(), TooManyAutomatonQueries> {
    let mut counter = AutomatonQueryCounter {
        remaining: max_automaton_queries,
    };
    counter.visit(query_ast)
}

/// Sentinel error, used to stop the traversal as soon as the budget of
/// automaton queries is exhausted.
struct TooManyAutomatonQueries;

struct AutomatonQueryCounter {
    remaining: usize,
}

impl AutomatonQueryCounter {
    fn count_one(&mut self) -> Result<(), TooManyAutomatonQueries> {
        if self.remaining == 0 {
            return Err(TooManyAutomatonQueries);
        }
        self.remaining -= 1;
        Ok(())
    }
}

impl<'a> QueryAstVisitor<'a> for AutomatonQueryCounter {
    type Err = TooManyAutomatonQueries;

    fn visit_regex(&mut self, _regex_query: &'a RegexQuery) -> Result<(), Self::Err> {
        self.count_one()
    }

    fn visit_wildcard(&mut self, _wildcard_query: &'a WildcardQuery) -> Result<(), Self::Err> {
        self.count_one()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_query::query_ast::{BoolQuery, TermQuery};

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

    fn term() -> QueryAst {
        TermQuery {
            field: "body".to_string(),
            value: "hello".to_string(),
        }
        .into()
    }

    #[test]
    fn test_query_complexity_factor_or_default() {
        let search_request = SearchRequest {
            query_ast: serde_json::to_string(&wildcard("*xyz")).unwrap(),
            ..Default::default()
        };
        assert_eq!(query_complexity_factor_or_default(&search_request), 125.0);
        let search_request = SearchRequest {
            query_ast: "not json".to_string(),
            ..Default::default()
        };
        assert_eq!(query_complexity_factor_or_default(&search_request), 1.0);
    }

    #[test]
    fn test_automaton_query_count_within_limit_is_accepted() {
        let ast: QueryAst = BoolQuery {
            should: vec![wildcard("*a*"), wildcard("*b*")],
            must_not: vec![regex(".*c.*")],
            ..Default::default()
        }
        .into();
        assert!(check_automaton_query_count_with_limit(&ast, 3).is_ok());
    }

    #[test]
    fn test_automaton_query_count_over_limit_is_rejected() {
        let ast: QueryAst = BoolQuery {
            should: vec![wildcard("*a*"), wildcard("*b*")],
            must_not: vec![regex(".*c.*")],
            ..Default::default()
        }
        .into();
        assert!(check_automaton_query_count_with_limit(&ast, 2).is_err());
    }

    #[test]
    fn test_automaton_query_count_ignores_other_leaves() {
        let ast: QueryAst = BoolQuery {
            must: vec![term(), QueryAst::MatchAll],
            ..Default::default()
        }
        .into();
        assert!(check_automaton_query_count_with_limit(&ast, 0).is_ok());
    }
}
