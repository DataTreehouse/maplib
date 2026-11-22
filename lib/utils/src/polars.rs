use crate::cancel::{self, Cancelled, CANCELLED_POLARS_MESSAGE};
use crate::fast_path::FastPath;
use polars::error::abort::catch_polars_abort;
use polars::prelude::{DslPlan, Engine, InProcessQuery, OptFlags, PolarsResult};
use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
use polars_async::executor::ALLOW_RAYON_THREADS;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::RecvTimeoutError;
use thiserror::Error;

#[cfg(feature = "pyo3")]
use pyo3::Python;
use std::time::Duration;
#[cfg(feature = "pyo3")]
use std::{thread::sleep, time::Instant};

#[derive(Error, Debug)]
pub enum InterruptableCollectError {
    #[error("interrupted via signal")]
    Interrupted,
    #[error(transparent)]
    Cancelled(#[from] Cancelled),
    #[error(transparent)]
    Polars(#[from] PolarsError),
}

/// Query results estimated to have at most this many rows are collected with the in-memory
/// engine. The streaming engine has a fixed cost of a few milliseconds per collect, which
/// dominates small queries such as lookups of one subject.
pub const SMALL_COLLECT_HEIGHT: usize = 65_536;

/// A join whose inputs have more rows than this multiplied together may give that many rows, so
/// a query that can be cancelled collects it with the streaming engine (see
/// [cancel::note_large_join]): the in-memory engine can not stop a join that is running.
pub const LARGE_JOIN_PRODUCT: usize = 1 << 24;

/// What is known about a query result before it is collected, for choosing the engine.
#[derive(Debug, Clone, Copy)]
enum CollectHint {
    /// Nothing.
    None,
    /// The result is estimated to have at most this many rows.
    Height(usize),
    /// The plan ends in a sort followed by a slice (ORDER BY with LIMIT).
    SortedSlice,
}

/// The engine used for query result collection. `MAPLIB_POLARS_ENGINE=in-memory` or `streaming`
/// selects that engine. Otherwise results estimated to be small (see [SMALL_COLLECT_HEIGHT]) and
/// sorted slices use the in-memory engine, and everything else the streaming engine.
fn query_engine(hint: CollectHint) -> Engine {
    match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
        Ok("in-memory") | Ok("in_memory") => Engine::InMemory,
        Ok("streaming") => Engine::Streaming,
        // Only the streaming engine stops a running join when the query is cancelled.
        _ if cancel::has_large_join() => Engine::Streaming,
        _ => match hint {
            CollectHint::Height(h) if h <= SMALL_COLLECT_HEIGHT => Engine::InMemory,
            CollectHint::SortedSlice => Engine::InMemory,
            _ => Engine::Streaming,
        },
    }
}

/// Turns off Polars' join reordering for `lf`. maplib orders the joins of a query itself, from
/// what the patterns share. Polars reorders them from row counts alone and does not see how
/// skewed the join keys are, so it can move a join on one of two shared variables first: in a
/// basic graph pattern where many solutions share a literal, e.g. `?a :name ?n . ?b :name ?n`
/// before `?a` is bound, that join is quadratic in the number of solutions. A plan takes its
/// optimizations from its leftmost input, so this is applied before every collect and to the
/// output of every join.
pub fn keep_join_order(lf: LazyFrame) -> LazyFrame {
    let flags = lf.get_current_optimizations();
    lf.with_optimizations(flags & !OptFlags::JOIN_ORDER)
}

/// Collects a lazy frame over data that is already in memory (or in storage files) with the
/// in-memory engine. Used for the storage work of adding, compacting and deleting triples, which
/// runs one item per table in parallel with Rayon. The in-memory engine is plain Rayon
/// fork-join, so it is safe on a Rayon worker. The streaming engine is not: its collect blocks
/// the thread in a way that lets Rayon hand it further items of the same loop, so streaming
/// queries nest until no thread is left to drive them and the add deadlocks. It also has a
/// fixed cost of a few milliseconds per collect, paid once or more per table.
pub fn collect_in_memory(lf: LazyFrame) -> Result<DataFrame, PolarsError> {
    keep_join_order(lf)
        .collect_with_engine(Engine::InMemory)
        .map(|r| r.unwrap_single())
}

/// Collects a lazy frame whose input has about `height` rows, outside of query evaluation (e.g.
/// a CONSTRUCT template or encoding a solution before it is inserted). Small inputs (see
/// [SMALL_COLLECT_HEIGHT]) use the in-memory engine, since the fixed cost of the streaming engine
/// dominates when few rows are handled at a time, as in each step of rule inference. Larger ones
/// are left to Polars. `MAPLIB_POLARS_ENGINE=streaming` or `in-memory` selects that engine.
pub fn collect_sized(lf: LazyFrame, height: usize) -> Result<DataFrame, PolarsError> {
    let engine = match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
        Ok("in-memory") | Ok("in_memory") => Engine::InMemory,
        Ok("streaming") => Engine::Auto,
        _ if height <= SMALL_COLLECT_HEIGHT => Engine::InMemory,
        _ => Engine::Auto,
    };
    let lf = keep_join_order(lf);
    // The steps of a small query (e.g. the solutions so far, collected to push their values
    // into the next pattern) are tiny plans: on Polars' pool, each of their joins waits for
    // threads to wake up, which takes longer than the join.
    if engine == Engine::InMemory && !on_pool_thread() && is_tiny_plan(&lf.logical_plan) {
        crate::fast_path::hit(FastPath::CollectTinyStep);
        return collect_tiny_plan(lf);
    }
    lf.collect_with_engine(engine).map(|r| r.unwrap_single())
}

/// Collects a lazy frame as [LazyFrame::collect] does, except that a tiny plan (see
/// [is_tiny_plan]) is collected on the helper thread of [collect_tiny].
///
/// Since Polars 0.55, [LazyFrame::collect] runs the streaming engine, whose fixed cost per
/// collect (starting its tasks, handing morsels between threads and waiting for them) is tens of
/// microseconds: more than the work of the plans that put a few rows in order, format them or
/// join them, of which a small query has several. `MAPLIB_POLARS_ENGINE=streaming` keeps the
/// streaming engine.
pub fn collect_auto(lf: LazyFrame) -> PolarsResult<DataFrame> {
    if streaming_forced() || on_pool_thread() || !is_tiny_plan(&lf.logical_plan) {
        return lf.collect();
    }
    crate::fast_path::hit(FastPath::CollectTinyStep);
    collect_tiny_plan(lf)
}

/// [collect_auto] as a method of a lazy frame.
pub trait CollectAuto {
    fn collect_auto(self) -> PolarsResult<DataFrame>;
}

impl CollectAuto for LazyFrame {
    fn collect_auto(self) -> PolarsResult<DataFrame> {
        collect_auto(self)
    }
}

/// Whether this thread is a worker of a rayon pool (Polars' or the global one). Such a thread
/// runs parallel steps without waiting for other threads to wake up, and there may be many of
/// them, so they do not get a helper thread each.
fn on_pool_thread() -> bool {
    rayon::current_thread_index().is_some()
}

fn streaming_forced() -> bool {
    std::env::var("MAPLIB_POLARS_ENGINE").as_deref() == Ok("streaming")
}

/// Collects a sort followed by a slice (ORDER BY with LIMIT) with the in-memory engine, many
/// times cheaper than the streaming engine. The working state is in memory, so such sorts do
/// not spill. `MAPLIB_POLARS_ENGINE=streaming` still selects the streaming engine.
pub fn pl_interruptable_collect_sorted_slice(
    lf: LazyFrame,
) -> Result<DataFrame, InterruptableCollectError> {
    collect_with_engine(lf, query_engine(CollectHint::SortedSlice))
}

/// collect a Polars LazyFrame in a way that allows a KeyboardInterrupt to cancel the search.
/// On the streaming engine this relies on Polars' abort mechanism, so the SIGINT hook must be
/// installed with `polars::error::abort::register_polars_abort_mechanism` at startup.
pub fn pl_interruptable_collect(lf: LazyFrame) -> Result<DataFrame, InterruptableCollectError> {
    collect_with_engine(lf, query_engine(CollectHint::None))
}

/// Like [pl_interruptable_collect], for a result estimated to have at most `height_estimate`
/// rows: small results are collected with the in-memory engine.
pub fn pl_interruptable_collect_estimated(
    lf: LazyFrame,
    height_estimate: usize,
) -> Result<DataFrame, InterruptableCollectError> {
    collect_with_engine(lf, query_engine(CollectHint::Height(height_estimate)))
}

fn collect_with_engine(
    lf: LazyFrame,
    engine: Engine,
) -> Result<DataFrame, InterruptableCollectError> {
    cancel::check()?;
    let lf = keep_join_order(cancel::cancel_check_node(lf));
    let result = collect_with_engine_unchecked(lf, engine);
    // A cancellation check in the plan fails the collect with a Polars error: report why.
    match result {
        Err(InterruptableCollectError::Polars(e)) => match cancel::stopped() {
            Some(c) => Err(c.into()),
            None if e.to_string().contains(CANCELLED_POLARS_MESSAGE) => {
                Err(Cancelled::Cancelled.into())
            }
            None => Err(e.into()),
        },
        r => r,
    }
}

fn collect_with_engine_unchecked(
    lf: LazyFrame,
    engine: Engine,
) -> Result<DataFrame, InterruptableCollectError> {
    match engine {
        Engine::InMemory => pl_interruptable_collect_in_memory(lf),
        engine => match cancel::current() {
            None => collect_streaming(lf, engine),
            Some(token) => collect_streaming_cancellable(lf, engine, token),
        },
    }
}

fn collect_streaming(
    lf: LazyFrame,
    engine: Engine,
) -> Result<DataFrame, InterruptableCollectError> {
    // A lazy frame without sinks always collects to a single DataFrame.
    let result = catch_polars_abort(AssertUnwindSafe(|| {
        lf.collect_with_engine(engine).map(|r| r.unwrap_single())
    }));
    match result {
        Ok(Ok(df)) => Ok(df),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(InterruptableCollectError::Interrupted),
    }
}

/// How often a cancellable streaming collect checks its token while it waits.
const STREAMING_CANCEL_POLL: Duration = Duration::from_millis(5);

/// Collects with the streaming engine on a thread of its own, and stops waiting for it as soon as
/// `token` stops. The cancellation checks in the plan (see [cancel::cancel_check_node]) fail the
/// query within a batch, but the streaming engine then waits for every task to finish the batch
/// it is working on, e.g. a filter of 100k rows from a cross join, which took 0.5 s to several
/// seconds on a loaded machine. Meanwhile the query held the model, so a timed out or abandoned
/// query blocked updates. The thread finishes those batches and drops the result, and counts as a
/// [cancel::Leftover] until then.
fn collect_streaming_cancellable(
    lf: LazyFrame,
    engine: Engine,
    token: cancel::CancelToken,
) -> Result<DataFrame, InterruptableCollectError> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let thread_token = token.clone();
    let leftover = cancel::Leftover::default();
    let thread_leftover = EndOnDrop(leftover.clone());
    let thread = std::thread::Builder::new()
        .name("maplib-collect".into())
        .spawn(move || {
            // Ends the leftover after the result was dropped, also on a panic.
            let _leftover = thread_leftover;
            let r = cancel::with_token(Some(thread_token), || collect_streaming(lf, engine));
            let _ = tx.send(r);
        })
        .map_err(|e| {
            PolarsError::ComputeError(format!("Could not start a thread for the query: {e}").into())
        })?;
    loop {
        match rx.recv_timeout(STREAMING_CANCEL_POLL) {
            Ok(r) => return r,
            Err(RecvTimeoutError::Timeout) => {
                if let Some(c) = token.stopped() {
                    leftover.leave();
                    return Err(c.into());
                }
            }
            // The thread panicked: so does the query, as it did when it collected on this thread.
            Err(RecvTimeoutError::Disconnected) => match thread.join() {
                Err(panic) => std::panic::resume_unwind(panic),
                Ok(()) => unreachable!("the collect thread sends its result before it ends"),
            },
        }
    }
}

/// A plan in which no step has more than this many rows is collected on one thread, see
/// [is_tiny_plan].
pub const TINY_COLLECT_ROWS: u64 = 16_384;

/// The most steps a plan collected on one thread may have, see [is_tiny_plan].
const TINY_COLLECT_STEPS: usize = 256;

/// Optimizations left out for tiny plans (see [is_tiny_plan]), where they cost more than they
/// save. They only change how a plan runs, not its result.
const TINY_PLAN_SKIPPED_OPTIMIZATIONS: OptFlags = OptFlags::COMM_SUBEXPR_ELIM
    .union(OptFlags::CLUSTER_WITH_COLUMNS)
    .union(OptFlags::CHECK_ORDER_OBSERVE);

/// Whether every step of `plan` is known to give at most [TINY_COLLECT_ROWS] rows: it reads only
/// in-memory frames, and joins of them give at most the product of their rows. Such a plan takes
/// well under a millisecond, which handing it to Polars' threads and polling for the result took
/// several times over: [pl_interruptable_collect_in_memory] then collects it on one thread.
/// Anything else, e.g. a scan of files, an explode or a Python function, is not tiny.
fn is_tiny_plan(plan: &DslPlan) -> bool {
    let mut steps = 0;
    tiny_plan_rows(plan, &mut steps).is_some()
}

/// The most rows `plan` gives, if every step of it gives at most [TINY_COLLECT_ROWS].
fn tiny_plan_rows(plan: &DslPlan, steps: &mut usize) -> Option<u64> {
    *steps += 1;
    if *steps > TINY_COLLECT_STEPS {
        return None;
    }
    let rows = match plan {
        DslPlan::DataFrameScan { df, .. } => df.height() as u64,
        DslPlan::Filter { input, .. }
        | DslPlan::Cache { input, .. }
        | DslPlan::Select { input, .. }
        | DslPlan::HStack { input, .. }
        | DslPlan::MatchToSchema { input, .. }
        | DslPlan::Distinct { input, .. }
        | DslPlan::Sort { input, .. }
        | DslPlan::GroupBy {
            input, apply: None, ..
        } => tiny_plan_rows(input, steps)?,
        DslPlan::Slice { input, len, .. } => tiny_plan_rows(input, steps)?.min(*len as u64),
        DslPlan::MapFunction { input, function } => {
            // Polars does not export the type of `function`, only its name.
            let name: &'static str = function.into();
            let keeps_rows = match name {
                "ROW_INDEX" | "RENAME" | "STATS" | "FILL_NAN" | "HINT" => true,
                "FUNCTION_IR" => function.to_string() == cancel::CANCEL_CHECK_NAME,
                _ => false,
            };
            if !keeps_rows {
                return None;
            }
            tiny_plan_rows(input, steps)?
        }
        DslPlan::Join {
            input_left,
            input_right,
            ..
        } => {
            let left = tiny_plan_rows(input_left, steps)?;
            let right = tiny_plan_rows(input_right, steps)?;
            // Also an outer join, which may give the rows of both sides that match nothing.
            left.saturating_mul(right).max(left.saturating_add(right))
        }
        DslPlan::Union { inputs, .. } => {
            let mut rows = 0u64;
            for i in inputs {
                rows = rows.saturating_add(tiny_plan_rows(i, steps)?);
            }
            rows
        }
        DslPlan::HConcat { inputs, .. } => {
            let mut rows = 0u64;
            for i in inputs {
                rows = rows.max(tiny_plan_rows(i, steps)?);
            }
            rows
        }
        // A plan that was converted to find its schema keeps what it was converted from.
        DslPlan::IR { dsl, .. } => tiny_plan_rows(dsl, steps)?,
        _ => return None,
    };
    (rows <= TINY_COLLECT_ROWS).then_some(rows)
}

type TinyJob = Box<dyn FnOnce() + Send>;

thread_local! {
    /// The thread that collects the tiny plans of this thread, see [collect_tiny].
    static TINY_COLLECTOR: std::cell::OnceCell<std::sync::mpsc::Sender<TinyJob>> =
        const { std::cell::OnceCell::new() };
}

/// Collects the tiny plan `lf` (see [is_tiny_plan]) with all of Polars' parallel steps on one
/// thread: for a tiny plan, waking the threads of Polars' pool and waiting for them takes longer
/// than the work.
///
/// Polars runs them on one thread when `ALLOW_RAYON_THREADS` is off, in a thread pool of one that
/// takes over the current thread: from then on, everything that thread runs in parallel with
/// rayon, also outside of Polars, runs on that thread alone. So it is not the calling thread, but
/// a helper thread of it that does nothing else, and waits for the next plan in between.
fn collect_tiny(lf: LazyFrame) -> PolarsResult<DataFrame> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let job: TinyJob = Box::new(move || {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            lf.collect_with_engine(Engine::InMemory)
                .map(|r| r.unwrap_single())
        }));
        let _ = tx.send(result);
    });
    TINY_COLLECTOR.with(|collector| {
        collector
            .get_or_init(|| {
                let (tx, rx) = std::sync::mpsc::channel::<TinyJob>();
                std::thread::Builder::new()
                    .name("maplib tiny collect".into())
                    .spawn(move || {
                        ALLOW_RAYON_THREADS.set(false);
                        // Ends when the thread it collects for has ended.
                        while let Ok(job) = rx.recv() {
                            job();
                        }
                    })
                    .expect("could not start the thread that collects tiny plans");
                tx
            })
            .send(job)
            .expect("the thread that collects tiny plans has stopped")
    });
    match rx.recv().expect("the thread that collects tiny plans has stopped") {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// [collect_tiny] without the optimizations that cost a tiny plan more than they save.
fn collect_tiny_plan(lf: LazyFrame) -> PolarsResult<DataFrame> {
    let flags = lf.get_current_optimizations();
    collect_tiny(lf.with_optimizations(flags & !TINY_PLAN_SKIPPED_OPTIMIZATIONS))
}

fn pl_interruptable_collect_in_memory(
    lf: LazyFrame,
) -> Result<DataFrame, InterruptableCollectError> {
    let tiny = is_tiny_plan(&lf.logical_plan);
    crate::fast_path::hit_if(tiny, FastPath::CollectTiny, FastPath::CollectConcurrent);
    if tiny {
        // It can not be interrupted, but stops before it starts if the query was stopped, and
        // a tiny plan uses next to no memory.
        return Ok(collect_tiny_plan(lf)?);
    }
    let future = lf.collect_concurrently()?;
    let token = cancel::current();

    #[cfg(feature = "pyo3")]
    {
        // The GIL is only held to check for signals, not while waiting: the query may need it
        // on another thread, e.g. to release buffers that Python owns (NumPy arrays behind the
        // results of a Python virtualized database), and would otherwise never finish.
        {
            // TODO: https://github.com/pola-rs/polars/issues/22513
            //                     min  med  avg  95%   99%   99.9%
            // check_signals takes 20 - 30 - 85 - 261 - 691 - 908
            // Signals are checked at most every SIGNAL_CHECK_INTERVAL. The result is polled with a
            // sleep that grows by a quarter up to MAX_POLL_SLEEP, so it is picked up at most a
            // quarter of its running time (or MAX_POLL_SLEEP) after it is ready. With a sleep that
            // doubled, a query of a few hundred microseconds often waited as long again, which
            // added up over the many small queries of each update or batch of a stream.
            const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);
            const MIN_POLL_SLEEP: Duration = Duration::from_micros(5);
            const MAX_POLL_SLEEP: Duration = Duration::from_micros(250);
            let mut sleeptime = MIN_POLL_SLEEP;
            let mut last_check = Instant::now();
            loop {
                match future.fetch() {
                    None => {
                        if let Some(c) = token.as_ref().and_then(|t| t.stopped()) {
                            abandon(future);
                            break Err(c.into());
                        }
                        let check = last_check.elapsed() >= SIGNAL_CHECK_INTERVAL;
                        if check {
                            last_check = Instant::now();
                        }
                        if check && Python::attach(|py| py.check_signals()).is_err() {
                            abandon(future);
                            break Err(InterruptableCollectError::Interrupted);
                        }
                        sleep(sleeptime);
                        sleeptime = std::cmp::min(sleeptime + sleeptime / 4, MAX_POLL_SLEEP);
                    }
                    Some(dfr) => break Ok(dfr?),
                }
            }
        }
    }

    #[cfg(not(feature = "pyo3"))]
    {
        let Some(token) = token else {
            return Ok(future.fetch_blocking()?);
        };
        let mut sleeptime = std::time::Duration::from_micros(10);
        loop {
            match future.fetch() {
                Some(dfr) => break Ok(dfr?),
                None => {
                    if let Some(c) = token.stopped() {
                        abandon(future);
                        break Err(c.into());
                    }
                    std::thread::sleep(sleeptime);
                    sleeptime = std::cmp::min(sleeptime * 2, std::time::Duration::from_millis(20));
                }
            }
        }
    }
}

/// Stops waiting for a query of the in-memory engine that was cancelled or interrupted.
///
/// The in-memory engine only stops between operators, so the operator that is running (e.g. a
/// large join) runs to its end. Polars then sends the result to the receiver of the query and
/// panics if the receiver is gone, which aborts the process from its Rayon thread
/// (<https://github.com/pola-rs/polars/issues/22515>). A thread keeps the receiver until then and
/// drops the result, so the query returns at once and its memory is freed when it stops.
/// The thread counts as a [cancel::Leftover] until then.
fn abandon(future: InProcessQuery) {
    future.cancel();
    let leftover = cancel::Leftover::default();
    leftover.leave();
    let leftover = EndOnDrop(leftover);
    std::thread::spawn(move || {
        let _leftover = leftover;
        let _ = future.fetch_blocking();
    });
}

/// Ends a [cancel::Leftover] when dropped.
struct EndOnDrop(cancel::Leftover);

impl Drop for EndOnDrop {
    fn drop(&mut self) {
        self.0.end();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::{col, IntoLazy, JoinArgs, JoinType};

    fn frame(rows: i64) -> LazyFrame {
        polars::df!("a" => (0..rows).collect::<Vec<_>>(), "b" => (0..rows).map(|i| i % 7).collect::<Vec<_>>())
            .unwrap()
            .lazy()
    }

    #[test]
    fn small_in_memory_plans_are_tiny() {
        let lf = frame(1000)
            .filter(col("b").gt(2))
            .group_by([col("b")])
            .agg([col("a").sum()])
            .rename(["a"], ["s"], true);
        assert!(is_tiny_plan(&lf.logical_plan));
        // Also with the cancellation check, and after its schema was looked up.
        let token = cancel::CancelToken::new(None);
        let mut lf = cancel::with_token(Some(token), || cancel_check_node_for_test(lf));
        lf.collect_schema().unwrap();
        assert!(is_tiny_plan(&lf.logical_plan));
        let joined = frame(100)
            .join(
                frame(100),
                [col("a")],
                [col("a")],
                JoinArgs::new(JoinType::Inner),
            )
            .unwrap();
        assert!(is_tiny_plan(&joined.logical_plan));
    }

    fn cancel_check_node_for_test(lf: LazyFrame) -> LazyFrame {
        cancel::cancel_check_node(lf)
    }

    #[test]
    fn large_or_unknown_plans_are_not_tiny() {
        assert!(!is_tiny_plan(
            &frame(TINY_COLLECT_ROWS as i64 + 1).logical_plan
        ));
        // A join may give the product of its inputs.
        let joined = frame(200)
            .join(
                frame(200),
                [col("b")],
                [col("b")],
                JoinArgs::new(JoinType::Inner),
            )
            .unwrap();
        assert!(!is_tiny_plan(&joined.logical_plan));
        let exploded = frame(10).select([col("a").implode(false)]).explode(
            polars::prelude::cols(["a"]),
            polars::prelude::ExplodeOptions {
                empty_as_null: false,
                keep_nulls: true,
            },
        );
        assert!(!is_tiny_plan(&exploded.logical_plan));
    }

    #[test]
    fn tiny_plans_collect_as_before() {
        let lf = || {
            frame(1000)
                .filter(col("b").gt(2))
                .group_by([col("b")])
                .agg([col("a").sum()])
                .sort(["b"], Default::default())
        };
        let token = cancel::CancelToken::new(None);
        let tiny = cancel::with_token(Some(token), || pl_interruptable_collect_estimated(lf(), 4))
            .unwrap();
        let other = lf()
            .collect_with_engine(Engine::Streaming)
            .unwrap()
            .unwrap_single();
        assert!(tiny.equals(&other));
        // A query that was stopped does not start.
        let token = cancel::CancelToken::new(None);
        token.cancel();
        assert!(matches!(
            cancel::with_token(Some(token), || pl_interruptable_collect_estimated(lf(), 4)),
            Err(InterruptableCollectError::Cancelled(_))
        ));
    }

    #[test]
    fn collecting_tiny_plans_leaves_the_calling_thread_to_rayon_as_it_was() {
        std::thread::spawn(|| {
            let lf = frame(100).group_by([col("b")]).agg([col("a").sum()]);
            assert!(is_tiny_plan(&lf.logical_plan));
            pl_interruptable_collect_estimated(lf, 4).unwrap();
            // Had the plan been collected in Polars' pool of one on this thread, it would have
            // made this thread its worker, and everything it runs in parallel would run on it.
            assert_eq!(rayon::current_thread_index(), None);
        })
        .join()
        .unwrap();
    }
}
