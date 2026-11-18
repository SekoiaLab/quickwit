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

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::InvalidQuery;

/// Bounds the memory of the automatons built for the regex and wildcard queries of a query.
///
/// Each regex or wildcard query compiles to a DFA whose size depends a lot on the pattern: a
/// case-insensitive pattern with a leading wildcard can take hundreds of KiB. Building the query
/// with a budget makes it fail as soon as its automatons exceed the budget, which bounds both the
/// memory and the CPU time spent compiling them. The budget can be exceeded by at most one
/// automaton, which the DFA state limit bounds to a few MiB.
#[derive(Debug)]
pub struct AutomatonMemoryBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
}

impl AutomatonMemoryBudget {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit_bytes,
            used_bytes: AtomicUsize::new(0),
        }
    }

    /// Bytes charged so far, including the automaton that exceeded the budget, if any.
    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Charges the memory of an automaton, and returns an error if the budget is exceeded.
    pub(crate) fn charge(&self, num_bytes: usize) -> Result<(), InvalidQuery> {
        let used_bytes = self.used_bytes.fetch_add(num_bytes, Ordering::Relaxed) + num_bytes;
        if used_bytes > self.limit_bytes {
            return Err(InvalidQuery::AutomatonMemoryLimitExceeded {
                limit_bytes: self.limit_bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_automaton_memory_budget() {
        let budget = AutomatonMemoryBudget::new(100);
        budget.charge(60).unwrap();
        budget.charge(40).unwrap();
        assert_eq!(budget.used_bytes(), 100);
        let error = budget.charge(1).unwrap_err();
        assert!(matches!(
            error,
            InvalidQuery::AutomatonMemoryLimitExceeded { limit_bytes: 100 }
        ));
        assert_eq!(budget.used_bytes(), 101);
    }
}
