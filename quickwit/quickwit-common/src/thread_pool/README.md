# Thread Pool Implementations

This module has two Rayon-backed executors for CPU work. Both record the same
metrics, labeled by pool, caller and cost class.

## `simple.rs`

`SimpleThreadPool` is the default small-task executor behind
`quickwit_common::thread_pool::run_cpu_intensive`.

It submits work directly to Rayon and keeps only the original cancellation
check: if the caller drops the returned future before Rayon starts executing
the closure, the closure is skipped. This implementation has the smallest
bookkeeping overhead and is meant for short CPU tasks such as decompression,
checksum computation, and small serialization work.

The `SimpleThreadPool` type is private. Callers should use the re-exported
`run_cpu_intensive` function for ordinary work, and
`small_tasks_tantivy_executor` for integration points, such as Tantivy's doc
store, that need a Tantivy executor instead of a future-returning submission
API.

## `with_priority.rs`

`ThreadPoolWithPriority` adds a Quickwit-owned queue in front of Rayon. It
keeps pending work outside Rayon until a worker slot is available, then
schedules high-priority tasks before normal-priority tasks. The worker that
completes a task schedules the next one, so dispatching does not depend on the
tokio runtime.

This is useful when a long queue of normal work can delay latency-sensitive
follow-up work. The search thread pool uses this implementation so final result
merges and object storage download assembly can jump ahead of queued
split-search CPU tasks.

The underlying Rayon pool is not exposed: Rayon runs the tasks dispatched from
a worker before work submitted to it from outside, so such work would be
starved while the queue has a backlog.
