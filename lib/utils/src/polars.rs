use crate::cancel::{self, Cancelled, CANCELLED_POLARS_MESSAGE};
use polars::error::abort::catch_polars_abort;
use polars::prelude::Engine;
use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
use std::panic::AssertUnwindSafe;
use thiserror::Error;

#[cfg(feature = "pyo3")]
use pyo3::Python;
#[cfg(feature = "pyo3")]
use std::{
    thread::sleep,
    time::{Duration, Instant},
};

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

/// The engine used for query result collection. `MAPLIB_POLARS_ENGINE=in-memory` or `streaming`
/// selects that engine. Otherwise results estimated to be small (see [SMALL_COLLECT_HEIGHT]) use
/// the in-memory engine and everything else the streaming engine.
fn query_engine(height_estimate: Option<usize>) -> Engine {
    match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
        Ok("in-memory") | Ok("in_memory") => Engine::InMemory,
        Ok("streaming") => Engine::Streaming,
        // Only the streaming engine stops a running join when the query is cancelled.
        _ if cancel::has_large_join() => Engine::Streaming,
        _ => match height_estimate {
            Some(h) if h <= SMALL_COLLECT_HEIGHT => Engine::InMemory,
            _ => Engine::Streaming,
        },
    }
}

/// Collects a lazy frame over data that is already in memory (or in storage files) with the
/// in-memory engine. Used for the storage work of adding, compacting and deleting triples, which
/// runs one item per table in parallel with Rayon. The in-memory engine is plain Rayon
/// fork-join, so it is safe on a Rayon worker. The streaming engine is not: its collect blocks
/// the thread in a way that lets Rayon hand it further items of the same loop, so streaming
/// queries nest until no thread is left to drive them and the add deadlocks. It also has a
/// fixed cost of a few milliseconds per collect, paid once or more per table.
pub fn collect_in_memory(lf: LazyFrame) -> Result<DataFrame, PolarsError> {
    lf.collect_with_engine(Engine::InMemory)
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
    lf.collect_with_engine(engine).map(|r| r.unwrap_single())
}

/// Collects a sort followed by a slice (ORDER BY with LIMIT) with the in-memory engine, many
/// times cheaper than the streaming engine. The working state is in memory, so such sorts do
/// not spill. `MAPLIB_POLARS_ENGINE=streaming` still selects the streaming engine.
pub fn pl_interruptable_collect_sorted_slice(
    lf: LazyFrame,
) -> Result<DataFrame, InterruptableCollectError> {
    let engine = match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
        Ok("streaming") => Engine::Streaming,
        _ if cancel::has_large_join() => Engine::Streaming,
        _ => Engine::InMemory,
    };
    collect_with_engine(lf, engine)
}

/// collect a Polars LazyFrame in a way that allows a KeyboardInterrupt to cancel the search.
/// On the streaming engine this relies on Polars' abort mechanism, so the SIGINT hook must be
/// installed with `polars::error::abort::register_polars_abort_mechanism` at startup.
pub fn pl_interruptable_collect(lf: LazyFrame) -> Result<DataFrame, InterruptableCollectError> {
    collect_with_engine(lf, query_engine(None))
}

/// Like [pl_interruptable_collect], for a result estimated to have at most `height_estimate`
/// rows: small results are collected with the in-memory engine.
pub fn pl_interruptable_collect_estimated(
    lf: LazyFrame,
    height_estimate: usize,
) -> Result<DataFrame, InterruptableCollectError> {
    collect_with_engine(lf, query_engine(Some(height_estimate)))
}

fn collect_with_engine(
    lf: LazyFrame,
    engine: Engine,
) -> Result<DataFrame, InterruptableCollectError> {
    cancel::check()?;
    let lf = cancel::cancel_check_node(lf);
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
        engine => {
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
    }
}

fn pl_interruptable_collect_in_memory(
    lf: LazyFrame,
) -> Result<DataFrame, InterruptableCollectError> {
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
            // sleep that doubles up to MAX_POLL_SLEEP, so it is picked up at most that long after
            // it is ready (with a cap of 50 ms, a query of a few milliseconds often waited as long
            // again, which added up over the many small queries of rule inference).
            const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(50);
            const MAX_POLL_SLEEP: Duration = Duration::from_millis(1);
            let mut sleeptime = Duration::from_nanos(15);
            let mut last_check = Instant::now();
            loop {
                sleep(sleeptime);
                match future.fetch() {
                    None => {
                        if let Some(c) = token.as_ref().and_then(|t| t.stopped()) {
                            future.cancel();
                            sleep(Duration::from_millis(50));
                            break Err(c.into());
                        }
                        let check = last_check.elapsed() >= SIGNAL_CHECK_INTERVAL;
                        if check {
                            last_check = Instant::now();
                        }
                        if check && Python::attach(|py| py.check_signals()).is_err() {
                            // Polars has some kind of race condition and panics as it tries to tx on our dropped rx
                            // We've already decided to bail out, so waiting a few ms for rayon to clean up or whatever should be fine
                            // TODO: https://github.com/pola-rs/polars/issues/22515
                            future.cancel();
                            sleep(Duration::from_millis(50));
                            break Err(InterruptableCollectError::Interrupted);
                        }
                        sleeptime = std::cmp::min(sleeptime * 2, MAX_POLL_SLEEP);
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
                        future.cancel();
                        break Err(c.into());
                    }
                    std::thread::sleep(sleeptime);
                    sleeptime = std::cmp::min(sleeptime * 2, std::time::Duration::from_millis(20));
                }
            }
        }
    }
}
