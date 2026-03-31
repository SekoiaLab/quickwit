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

use quickwit_proto::search::SortOrder;
use tantivy::DocId;

use crate::top_k_computer::MinValue;

/// Encoded representation of the value, the index of its accessor in the list
/// of fast field columns and the sort order.
///
/// The first u8 encodes the index of the accessor and a sentinel value for
/// missing and search after values:
/// - 0 is a sentinel for skip all
/// - 1 is a sentinel for missing (always last in the sort order)
/// - other odd values encode the index of the accessor in the list of fast field columns (3 for
///   index 0, 5 for index 1, etc.)
/// - even values are sentinels for search after values that keep/skip all documents for a given
///   column (2 to skip all columns but keep missing, 4 only keeps column 0, 6 keeps column 0 and 1,
///   etc.)
///
/// The following u64 encodes the value itself or its bitwise negation to
/// reverse the sort order when building an ascending sort (keeping in mind that
/// this is fed to a top-k calculator).
#[derive(Clone, Copy)]
pub(crate) struct InternalValueRepr(u8, u64);

impl InternalValueRepr {
    #[inline]
    pub fn new(value: u64, accessor_idx: u8, order: SortOrder) -> Self {
        // For Asc, smaller values should win: invert so smaller maps to larger repr
        match order {
            SortOrder::Asc => Self(!(accessor_idx * 2 + 3), !value),
            SortOrder::Desc => Self(accessor_idx * 2 + 3, value),
        }
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be kept.
    pub fn new_keep_column(accessor_idx: u8, order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => Self(!(accessor_idx * 2 + 2), 0),
            SortOrder::Desc => Self(accessor_idx * 2 + 4, 0),
        }
    }
    #[inline]
    pub fn new_missing() -> Self {
        // Missing always last in topk, so use the smallest possible value
        // (besides the skip_all value)
        Self(1, 0)
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be skipped for the given column.
    pub fn new_skip_column(accessor_idx: u8, order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => Self(!(accessor_idx * 2 + 4), 0),
            SortOrder::Desc => Self(accessor_idx * 2 + 2, 0),
        }
    }
    /// A sentinel value that can be instantiated as search after boundary to indicate
    /// that all documents should be skipped.
    pub fn new_skip_all_but_missing() -> Self {
        Self(2, 0)
    }
    #[inline]
    pub fn decode(self, order: SortOrder) -> Option<(u8, u64)> {
        if self.0 == 1 {
            return None;
        }
        debug_assert_eq!(
            match order {
                SortOrder::Asc => !self.0,
                SortOrder::Desc => self.0,
            } % 2,
            1,
            "sentinel indexes are not meant to be decoded"
        );
        match order {
            SortOrder::Asc => Some(((!self.0 - 3) / 2, !self.1)),
            SortOrder::Desc => Some(((self.0 - 3) / 2, self.1)),
        }
    }
}

/// This is the ordered representation of the sort values. It is the
/// concatenation of:
/// - the first two (u8, u64) pairs contain the internal representation of the sort values
/// - the second sort value's internal representation
/// - the doc id, preceeded by a sentinel indicating how it should be used for tie-breaking
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub(crate) struct InternalSortValueRepr(u8, u64, u8, u64, u8, u32);

impl InternalSortValueRepr {
    #[inline]
    pub fn new(
        sort_1: InternalValueRepr,
        sort_2: InternalValueRepr,
        doc_id: DocId,
        doc_id_sort: SortOrder,
    ) -> Self {
        // For Asc, smaller values should win: invert so smaller maps to larger repr
        match doc_id_sort {
            SortOrder::Asc => Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 1, !doc_id),
            SortOrder::Desc => Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 1, doc_id),
        }
    }
    pub fn new_keep_doc_ids(sort_1: InternalValueRepr, sort_2: InternalValueRepr) -> Self {
        Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 2, 0)
    }
    pub fn new_skip_doc_ids(sort_1: InternalValueRepr, sort_2: InternalValueRepr) -> Self {
        Self(sort_1.0, sort_1.1, sort_2.0, sort_2.1, 0, 0)
    }
    #[inline]
    pub fn sort_1(self) -> InternalValueRepr {
        InternalValueRepr(self.0, self.1)
    }
    #[inline]
    pub fn sort_2(self) -> InternalValueRepr {
        InternalValueRepr(self.2, self.3)
    }
    #[inline]
    pub fn doc_id(self, order: SortOrder) -> DocId {
        debug_assert_eq!(self.4, 1, "doc id sentinel is not meant to be decoded");
        match order {
            SortOrder::Asc => !self.5,
            SortOrder::Desc => self.5,
        }
    }
}

impl MinValue for InternalSortValueRepr {
    fn min_value() -> Self {
        Self(0, 0, 0, 0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_sort_value_repr_ordering_values() {
        // Primary sort (Desc v1=10) dominates over secondary (Desc v2=100) and doc_id.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 0, SortOrder::Desc),
            InternalValueRepr::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "primary sort must dominate, desc");

        // Same values but Asc, the order is reversed
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Asc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 0, SortOrder::Asc),
            InternalValueRepr::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "primary sort must dominate, asc");

        // Secondary sort (Desc v2) breaks a tie on the primary field.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new(5, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "secondary sort must break primary tie, desc");

        // Same values but Asc, the order is reversed.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new(10, 0, SortOrder::Asc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new(5, 0, SortOrder::Asc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "secondary sort must break primary tie, asc");

        // Doc-id Desc tiebreaker: higher doc_id wins.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new_missing(),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new_missing(),
            5,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "Desc: higher doc_id must win tiebreaker");

        // Doc-id Asc tiebreaker: lower doc_id wins.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new_missing(),
            5,
            SortOrder::Asc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            InternalValueRepr::new_missing(),
            10,
            SortOrder::Asc,
        );
        assert!(lhs > rhs, "Asc: lower doc_id must win tiebreaker");

        // Missing values are always smaller
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new_missing(),
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 0, SortOrder::Desc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "missing values are always smaller, desc");

        // Same but Asc, missing is still smaller.
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new_missing(),
            InternalValueRepr::new(10, 0, SortOrder::Desc),
            10,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 0, SortOrder::Asc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "missing values are always smaller, asc");
    }

    #[test]
    fn test_internal_sort_value_repr_ordering_sentinels() {
        // Doc-id sentinel ordering: skip_doc_ids < normal_doc_id < keep_doc_ids.
        let s1 = InternalValueRepr::new(10, 0, SortOrder::Desc);
        let s2 = InternalValueRepr::new_missing();
        let skip_docs = InternalSortValueRepr::new_skip_doc_ids(s1, s2);
        let keep_docs = InternalSortValueRepr::new_keep_doc_ids(s1, s2);
        let normal_doc_desc = InternalSortValueRepr::new(s1, s2, 0, SortOrder::Desc);
        let normal_doc_asc = InternalSortValueRepr::new(s1, s2, 0, SortOrder::Asc);
        assert!(
            skip_docs < normal_doc_desc,
            "skip_doc_ids must be below normal"
        );
        assert!(
            normal_doc_desc < keep_docs,
            "normal must be below keep_doc_ids"
        );
        assert!(
            skip_docs < normal_doc_asc,
            "skip_doc_ids must be below normal"
        );
        assert!(
            normal_doc_asc < keep_docs,
            "normal must be below keep_doc_ids"
        );
    }

    #[test]
    fn test_internal_sort_value_repr_ordering_types() {
        // Primary accessor ordering dominates all the rest
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 1, SortOrder::Desc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(15, 0, SortOrder::Desc),
            InternalValueRepr::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs > rhs, "primary type sort must dominate, desc");

        // Same values but Asc, the order is reversed
        let lhs = InternalSortValueRepr::new(
            InternalValueRepr::new(5, 1, SortOrder::Asc),
            InternalValueRepr::new(0, 0, SortOrder::Desc),
            0,
            SortOrder::Desc,
        );
        let rhs = InternalSortValueRepr::new(
            InternalValueRepr::new(15, 0, SortOrder::Asc),
            InternalValueRepr::new(100, 0, SortOrder::Desc),
            999,
            SortOrder::Desc,
        );
        assert!(lhs < rhs, "primary type sort must dominate, asc");
    }

    #[test]
    fn test_memory_footprint() {
        // Make sure that the memory representation is efficiently packed. For
        // instance refactoring to:
        // ```
        //   struct InternalSortValueRepr(InternalValueRepr,InternalValueRepr, u64)
        // ```
        // would cause the size to jump to 40 bytes.
        assert_eq!(std::mem::size_of::<InternalSortValueRepr>(), 24);
    }
}
