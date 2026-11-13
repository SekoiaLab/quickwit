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

use std::fmt::Write;
use std::sync::OnceLock;

use regex::Regex;
use warp::Filter;

fn remove_trailing_numbers(thread_name: &mut String) {
    static REMOVE_TRAILING_NUMBER_PTN: OnceLock<Regex> = OnceLock::new();
    let captures_opt = REMOVE_TRAILING_NUMBER_PTN
        .get_or_init(|| Regex::new(r"^(.*?)[-\d]+$").unwrap())
        .captures(thread_name);
    if let Some(captures) = captures_opt {
        *thread_name = captures[1].to_string();
    }
}

fn frames_post_processor(frames: &mut pprof::Frames) {
    remove_trailing_numbers(&mut frames.thread_name);
}

/// Renders a report as collapsed (a.k.a. folded) stacks: one line per unique stack,
/// formatted as `thread;frame;frame;… <sample count>`, hottest stack first.
///
/// This is the very same representation the flamegraph is built from, only kept as
/// plain text: it can be grepped, aggregated and diffed between two runs, whereas
/// the SVG requires recovering the call tree from the geometry of its rectangles.
fn folded_stacks(report: &pprof::Report) -> String {
    let mut stacks: Vec<(isize, String)> = Vec::with_capacity(report.data.len());
    for (frames, sample_count) in &report.data {
        let mut stack = frames.thread_name_or_id();
        // Frames are captured from the leaf up, but the folded format expects them
        // from the root down.
        for frame in frames.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                let _ = write!(&mut stack, ";{symbol}");
            }
        }
        stacks.push((*sample_count, stack));
    }
    // Sorting by decreasing sample count puts the stacks that matter first. The stack
    // itself breaks ties so that two profiles of the same workload stay comparable
    // (`report.data` is a `HashMap`, its iteration order is not stable).
    stacks.sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let mut folded = String::new();
    for (sample_count, stack) in stacks {
        let _ = writeln!(&mut folded, "{stack} {sample_count}");
    }
    folded
}

/// pprof/start to start cpu profiling.
/// pprof/start?duration=5&sampling=1000 to start a short high frequency cpu profiling
/// pprof/flamegraph to return the last flamegraph
/// pprof/folded to return the last profile as collapsed stacks (text/plain)
///
/// Note that neither `pprof/flamegraph` nor `pprof/folded` stops an ongoing profiling:
/// the profiler runs for `duration` seconds, and only then are both representations
/// rendered and made available. Polling them before that returns the previous run, if
/// any.
///
/// Query parameters:
/// - duration: duration of the profiling in seconds, default is 30 seconds. max value is 300
/// - sampling: the sampling rate, default is 100, max value is 1000
pub fn pprof_handlers() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone
{
    use std::sync::{Arc, Mutex};

    use pprof::ProfilerGuard;
    use serde::Deserialize;
    use tokio::time::{self, Duration};
    use warp::reply::Reply;

    struct ProfilerState {
        profiler_guard: Option<ProfilerGuard<'static>>,
        // We will keep the latest report and return it at the flamegraph and folded
        // endpoints. A new run will overwrite both.
        flamegraph_data: Option<Vec<u8>>,
        folded_data: Option<String>,
    }

    let profiler_state = Arc::new(Mutex::new(ProfilerState {
        profiler_guard: None,
        flamegraph_data: None,
        folded_data: None,
    }));

    #[derive(Deserialize)]
    struct ProfilerQueryParams {
        duration: Option<u64>, // max allowed value is 300 seconds, default is 30 seconds
        sampling: Option<i32>, // max value is 1000, default is 100
    }

    let start_profiler = {
        let profiler_state = Arc::clone(&profiler_state);
        warp::path!("pprof" / "start")
            .and(warp::query::<ProfilerQueryParams>())
            .and_then(move |params: ProfilerQueryParams| {
                start_profiler_handler(profiler_state.clone(), params)
            })
    };

    let get_flamegraph = {
        let profiler_state = Arc::clone(&profiler_state);
        warp::path!("pprof" / "flamegraph")
            .and_then(move || get_flamegraph_handler(Arc::clone(&profiler_state)))
    };

    let get_folded = {
        let profiler_state = Arc::clone(&profiler_state);
        warp::path!("pprof" / "folded")
            .and_then(move || get_folded_handler(Arc::clone(&profiler_state)))
    };

    async fn start_profiler_handler(
        profiler_state: Arc<Mutex<ProfilerState>>,
        params: ProfilerQueryParams,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let mut state = profiler_state.lock().unwrap();

        if state.profiler_guard.is_none() {
            let duration = params.duration.unwrap_or(30).min(300);
            let sampling = params.sampling.unwrap_or(100).min(1000);
            state.profiler_guard = Some(pprof::ProfilerGuard::new(sampling).unwrap());
            let profiler_state = Arc::clone(&profiler_state);
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(duration)).await;
                save_report(profiler_state).await;
            });
            Ok(warp::reply::with_status(
                "CPU profiling started",
                warp::http::StatusCode::OK,
            ))
        } else {
            Ok(warp::reply::with_status(
                "CPU profiling is already running",
                warp::http::StatusCode::BAD_REQUEST,
            ))
        }
    }

    async fn get_flamegraph_handler(
        profiler_state: Arc<Mutex<ProfilerState>>,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let state = profiler_state.lock().unwrap();

        if let Some(data) = state.flamegraph_data.clone() {
            Ok(warp::reply::with_header(data, "Content-Type", "image/svg+xml").into_response())
        } else {
            Ok(warp::reply::with_status(
                "flamegraph is not available",
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
    }

    async fn get_folded_handler(
        profiler_state: Arc<Mutex<ProfilerState>>,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        let state = profiler_state.lock().unwrap();

        if let Some(data) = state.folded_data.clone() {
            Ok(
                warp::reply::with_header(data, "Content-Type", "text/plain; charset=utf-8")
                    .into_response(),
            )
        } else {
            Ok(warp::reply::with_status(
                "folded stacks are not available",
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
    }

    async fn save_report(profiler_state: Arc<Mutex<ProfilerState>>) {
        let handle = quickwit_common::thread_pool::run_cpu_intensive(move || {
            let mut state = profiler_state.lock().unwrap();
            if let Some(profiler) = state.profiler_guard.take()
                && let Ok(report) = profiler
                    .report()
                    .frames_post_processor(frames_post_processor)
                    .build()
            {
                state.folded_data = Some(folded_stacks(&report));
                let mut buffer = Vec::new();
                if report.flamegraph(&mut buffer).is_ok() {
                    state.flamegraph_data = Some(buffer);
                }
            }
        });
        let _ = handle.await;
    }

    start_profiler.or(get_flamegraph).or(get_folded)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{folded_stacks, remove_trailing_numbers};

    fn symbol(name: &str) -> pprof::Symbol {
        pprof::Symbol {
            name: Some(name.as_bytes().to_vec()),
            addr: None,
            lineno: None,
            filename: None,
        }
    }

    /// Builds a stack from its frames given from the root down, the way the folded
    /// format prints them (`pprof::Frames` stores them the other way around).
    fn frames(thread_name: &str, root_to_leaf: &[&str]) -> pprof::Frames {
        pprof::Frames {
            frames: root_to_leaf
                .iter()
                .rev()
                .map(|name| vec![symbol(name)])
                .collect(),
            thread_name: thread_name.to_string(),
            thread_id: 1,
            sample_timestamp: std::time::SystemTime::UNIX_EPOCH,
        }
    }

    fn report(stacks: Vec<(pprof::Frames, isize)>) -> pprof::Report {
        pprof::Report {
            data: HashMap::from_iter(stacks),
            timing: Default::default(),
        }
    }

    #[test]
    fn test_folded_stacks() {
        let folded = folded_stacks(&report(vec![
            (frames("quickwit-search", &["search", "warmup"]), 3),
            (frames("quickwit-search", &["search", "collect"]), 12),
        ]));
        // Hottest stack first, frames from the root down, count last.
        assert_eq!(
            folded,
            "quickwit-search;search;collect 12\nquickwit-search;search;warmup 3\n"
        );
    }

    #[test]
    fn test_folded_stacks_is_ordered_deterministically() {
        // Same count: the stack itself breaks the tie, so that two profiles of the
        // same workload can be diffed.
        let folded = folded_stacks(&report(vec![
            (frames("thread", &["b"]), 1),
            (frames("thread", &["a"]), 1),
        ]));
        assert_eq!(folded, "thread;a 1\nthread;b 1\n");
    }

    #[test]
    fn test_folded_stacks_empty_report() {
        assert_eq!(folded_stacks(&report(Vec::new())), "");
    }

    #[test]
    fn test_folded_stacks_inlined_frames() {
        // A single frame can resolve to several symbols when a call was inlined: they
        // are printed as regular frames, the innermost one last.
        let mut inlined = frames("thread", &["outer"]);
        inlined
            .frames
            .insert(0, vec![symbol("inlined"), symbol("caller")]);
        assert_eq!(
            folded_stacks(&report(vec![(inlined, 5)])),
            "thread;outer;caller;inlined 5\n"
        );
    }

    #[track_caller]
    fn test_remove_trailing_numbers_aux(thread_name: &str, expected: &str) {
        let mut thread_name = thread_name.to_string();
        remove_trailing_numbers(&mut thread_name);
        assert_eq!(&thread_name, expected);
    }

    #[test]
    fn test_remove_trailing_numbers() {
        test_remove_trailing_numbers_aux("thread-12", "thread");
        test_remove_trailing_numbers_aux("thread12", "thread");
        test_remove_trailing_numbers_aux("thread-", "thread");
        test_remove_trailing_numbers_aux("thread-1-2", "thread");
        test_remove_trailing_numbers_aux("thread-1-2", "thread");
        test_remove_trailing_numbers_aux("12-aa", "12-aa");
    }
}
