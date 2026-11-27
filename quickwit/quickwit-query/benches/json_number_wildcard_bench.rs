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

//! Benchmarks the wildcards matched against the numbers of a JSON field.
//!
//! - `terms/*`: number terms fed byte by byte through `JsonNumberWildcardAutomaton`, as the term
//!   dictionary streamer does: the cost of matching a number, per term.
//! - `walk/*`: an `AutomatonQuery` counting the documents of an in-memory segment: the term
//!   dictionary walk, the postings and the matching together.
//!
//! To compare a change against the current code:
//! ```text
//! cargo bench -p quickwit-query --bench json_number_wildcard_bench -- --save-baseline before
//! # apply the change
//! cargo bench -p quickwit-query --bench json_number_wildcard_bench -- --baseline before
//! ```

use std::hint::black_box;
use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use quickwit_query::JsonPath;
use quickwit_query::query_ast::{
    AutomatonQuery, JsonNumberWildcardAutomaton, JsonPathPrefix, WildcardQuery,
};
use tantivy::collector::Count;
use tantivy::schema::{
    Field, IndexRecordOption, JsonObjectOptions, Schema, TextFieldIndexing, Type,
};
use tantivy::{Index, IndexWriter, Searcher, TantivyDocument, f64_to_u64, i64_to_u64};
use tantivy_fst::Automaton;

// The searchers run on jemalloc: the cost of an allocation depends on the allocator.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const NUM_DOCS: usize = 1_000_000;
const NUM_TERMS: usize = 10_000;

/// The number fields of the documents, by JSON path.
#[derive(Clone, Copy)]
enum Values {
    /// HTTP status codes: ~500 distinct values.
    Status,
    /// Ports: ~65k distinct values.
    Port,
    /// Byte counts up to 1 TB: 1 to 13 digits, ~1 distinct value per document.
    Bytes,
    /// Integers above `i64::MAX`, indexed as `u64`: 19 to 20 digits.
    Big,
    /// Non-integral floats, the only ones indexed as `f64`.
    Latency,
}

impl Values {
    const ALL: [Values; 5] = [
        Values::Status,
        Values::Port,
        Values::Bytes,
        Values::Big,
        Values::Latency,
    ];

    fn path(self) -> &'static str {
        match self {
            Values::Status => "status",
            Values::Port => "port",
            Values::Bytes => "bytes",
            Values::Big => "big",
            Values::Latency => "latency",
        }
    }

    fn sample(self, rng: &mut XorShift) -> serde_json::Value {
        match self {
            Values::Status => (100 + rng.next() % 500).into(),
            Values::Port => (rng.next() % 65_536).into(),
            Values::Bytes => (rng.next() % 1_000_000_000_000).into(),
            Values::Big => (i64::MAX as u64 + 1 + rng.next() % (u64::MAX / 2)).into(),
            Values::Latency => ((rng.next() % 10_000_000) as f64 / 1000.0 + 0.0005).into(),
        }
    }

    /// The term of the value after the JSON path: the type code and the 8 bytes of the value.
    fn encode(value: &serde_json::Value) -> [u8; 9] {
        let (value_type, bytes) = if let Some(int) = value.as_i64() {
            (Type::I64, i64_to_u64(int))
        } else if let Some(uint) = value.as_u64() {
            (Type::U64, uint)
        } else {
            (Type::F64, f64_to_u64(value.as_f64().unwrap()))
        };
        let mut term = [0u8; 9];
        term[0] = value_type.to_code();
        term[1..].copy_from_slice(&bytes.to_be_bytes());
        term
    }
}

/// Deterministic xorshift, to avoid depending on `rand`.
struct XorShift(u64);

impl XorShift {
    fn new(values: Values) -> Self {
        XorShift(0x9E37_79B9_7F4A_7C15 ^ (values as u64 + 1))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Returns the JSON path prefix shared by the number terms of the path, and the automaton matching
/// their text against the wildcard: the ones the leaf builds for a wildcard query.
fn number_automaton(path: &str, wildcard: &str) -> (JsonPath, JsonNumberWildcardAutomaton) {
    let wildcard_query = WildcardQuery {
        field: format!("attrs.{path}"),
        value: wildcard.to_string(),
        lenient: false,
        case_insensitive: false,
    };
    let (prefix, number_wildcard) = wildcard_query
        .to_json_number_wildcard(Some(&JsonPath::from_json_path(path, false)))
        .expect("a number can match the wildcard");
    (prefix, number_wildcard.to_automaton().unwrap())
}

/// Feeds the term to the automaton byte by byte and returns whether it matches.
fn term_matches(automaton: &JsonNumberWildcardAutomaton, term: &[u8]) -> bool {
    let mut state = automaton.start();
    for &byte in term {
        state = automaton.accept(&state, byte);
    }
    automaton.is_match(&state)
}

fn bench_terms(c: &mut Criterion) {
    let cases: [(Values, &[&str]); 5] = [
        (Values::Status, &["2??", "*04"]),
        (Values::Port, &["2??", "1*5", "*04"]),
        (Values::Bytes, &["1*", "1*5", "*04"]),
        (Values::Big, &["1*5", "*04"]),
        (Values::Latency, &["1*5", "*.5*"]),
    ];
    for (values, wildcards) in cases {
        let mut rng = XorShift::new(values);
        let terms: Vec<[u8; 9]> = (0..NUM_TERMS)
            .map(|_| Values::encode(&values.sample(&mut rng)))
            .collect();
        let mut group = c.benchmark_group(format!("terms/{}", values.path()));
        group.throughput(Throughput::Elements(terms.len() as u64));
        for wildcard in wildcards {
            let (_, automaton) = number_automaton(values.path(), wildcard);
            group.bench_function(*wildcard, |b| {
                b.iter(|| {
                    terms
                        .iter()
                        .filter(|term| term_matches(&automaton, black_box(&term[..])))
                        .count()
                })
            });
        }
        group.finish();
    }
}

fn build_index() -> (Searcher, Field) {
    let mut schema_builder = Schema::builder();
    // The indexing of the dynamic fields of a doc mapping.
    let json_options = JsonObjectOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_index_option(IndexRecordOption::Basic),
    );
    let field = schema_builder.add_json_field("attrs", json_options);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer: IndexWriter = index.writer_with_num_threads(1, 2_000_000_000).unwrap();
    let mut rngs: Vec<(Values, XorShift)> = Values::ALL
        .iter()
        .map(|&values| (values, XorShift::new(values)))
        .collect();
    for _ in 0..NUM_DOCS {
        let attrs: serde_json::Map<String, serde_json::Value> = rngs
            .iter_mut()
            .map(|(values, rng)| (values.path().to_string(), values.sample(rng)))
            .collect();
        let doc = serde_json::json!({ "attrs": attrs });
        writer
            .add_document(TantivyDocument::parse_json(&schema, &doc.to_string()).unwrap())
            .unwrap();
    }
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    let searcher = index.reader().unwrap().searcher();
    (searcher, field)
}

fn bench_walk(c: &mut Criterion) {
    let (searcher, field) = build_index();
    let cases: [(Values, &[&str]); 5] = [
        // Few terms: the matching is negligible.
        (Values::Status, &["2??"]),
        (Values::Port, &["*04"]),
        // `2??` skips every block of the path, `1*` only keeps the integers it matches.
        (Values::Bytes, &["2??", "1*", "1*5", "*04"]),
        (Values::Big, &["*04"]),
        (Values::Latency, &["*.5*"]),
    ];
    for (values, wildcards) in cases {
        for wildcard in wildcards {
            let (prefix, automaton) = number_automaton(values.path(), wildcard);
            let query = AutomatonQuery {
                field,
                automaton: Arc::new(JsonPathPrefix {
                    prefix,
                    automaton: Arc::new(automaton),
                }),
            };
            let mut group = c.benchmark_group(format!("walk/{}", values.path()));
            group.sample_size(20);
            group.bench_function(*wildcard, |b| {
                b.iter(|| searcher.search(&query, &Count).unwrap())
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench_terms, bench_walk);
criterion_main!(benches);
