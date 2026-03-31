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

use std::cmp::Ordering;

use quickwit_proto::search::PartialHit;
use quickwit_proto::types::SplitId;
use tantivy::{DocId, Score, SegmentOrdinal};

use crate::collector::SortingFieldExtractorPair;
use crate::sort_repr::InternalSortValueRepr;
use crate::top_k_computer::TopKComputer;

pub struct QuickwitSegmentTopKCollector {
    split_id: SplitId,
    // We track the segment ordinal here, but splits only have 1 segment so this
    // should always be 0.
    segment_ord: SegmentOrdinal,
    hit_fetcher: SortingFieldExtractorPair,
    top_k_hits: TopKComputer<InternalSortValueRepr>,
    search_after_opt: Option<InternalSortValueRepr>,
}

impl QuickwitSegmentTopKCollector {
    pub fn new(
        split_id: SplitId,
        segment_ord: SegmentOrdinal,
        hit_fetcher: SortingFieldExtractorPair,
        top_k: usize,
        search_after_opt: Option<InternalSortValueRepr>,
    ) -> Self {
        QuickwitSegmentTopKCollector {
            split_id,
            segment_ord,
            top_k_hits: TopKComputer::new(top_k),
            hit_fetcher,
            search_after_opt,
        }
    }

    pub(crate) fn collect_top_k_block(&mut self, docs: &[DocId]) {
        let search_after_opt = self.search_after_opt;
        let top_k_hits = &mut self.top_k_hits;
        self.hit_fetcher
            .project_to_internal_sort_value_block(docs, |repr| {
                if let Some(search_after) = search_after_opt
                    && repr.cmp(&search_after) != Ordering::Less
                {
                    return;
                }
                top_k_hits.push(repr);
            });
    }

    pub(crate) fn collect_top_k(&mut self, doc_id: DocId, score: Score) {
        let internal_repr = self
            .hit_fetcher
            .project_to_internal_sort_value(doc_id, score);
        if let Some(search_after) = self.search_after_opt
            && internal_repr.cmp(&search_after) != Ordering::Less
        {
            return;
        }
        self.top_k_hits.push(internal_repr);
    }

    pub(crate) fn get_top_k(&self) -> tantivy::Result<Vec<PartialHit>> {
        self.top_k_hits
            .clone()
            .into_sorted_vec()
            .into_iter()
            .map(|internal_repr| {
                self.hit_fetcher.internal_to_partial_hit(
                    &self.split_id,
                    self.segment_ord,
                    internal_repr,
                )
            })
            .collect()
    }
}
