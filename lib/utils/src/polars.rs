use polars::error::abort::catch_polars_abort;
use polars::prelude::Engine;
use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
use std::panic::AssertUnwindSafe;
use thiserror::Error;

#[cfg(feature = "pyo3")]
use pyo3::Python;
#[cfg(feature = "pyo3")]
use std::{thread::sleep, time::Duration};

#[derive(Error, Debug)]
pub enum InterruptableCollectError {
    #[error("interrupted via signal")]
    Interrupted,
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

/// Collects a sort followed by a slice (ORDER BY with LIMIT) with the in-memory engine, many
/// times cheaper than the streaming engine. The working state is in memory, so such sorts do
/// not spill. `MAPLIB_POLARS_ENGINE=streaming` still selects the streaming engine.
pub fn pl_interruptable_collect_sorted_slice(
    lf: LazyFrame,
) -> Result<DataFrame, InterruptableCollectError> {
    let engine = match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
        Ok("streaming") => Engine::Streaming,
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

    #[cfg(feature = "pyo3")]
    {
        Python::attach(move |py| {
            // TODO: https://github.com/pola-rs/polars/issues/22513
            //                     min  med  avg  95%   99%   99.9%
            // check_signals takes 20 - 30 - 85 - 261 - 691 - 908
            // It's better to give it some time rather than waste it on checking signals
            let mut sleeptime = Duration::from_nanos(15);
            loop {
                sleep(sleeptime);
                match future.fetch() {
                    None => {
                        if py.check_signals().is_err() {
                            // Polars has some kind of race condition and panics as it tries to tx on our dropped rx
                            // We've already decided to bail out, so waiting a few ms for rayon to clean up or whatever should be fine
                            // TODO: https://github.com/pola-rs/polars/issues/22515
                            future.cancel();
                            sleep(Duration::from_millis(50));
                            break Err(InterruptableCollectError::Interrupted);
                        }
                        // Max delay is twice the time the query actually took
                        sleeptime = std::cmp::min(sleeptime * 2, Duration::from_millis(50));
                    }
                    Some(dfr) => break Ok(dfr?),
                }
            }
        })
    }

    #[cfg(not(feature = "pyo3"))]
    Ok(future.fetch_blocking()?)
}
