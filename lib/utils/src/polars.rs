use polars::error::abort::{catch_polars_abort, register_polars_abort_mechanism};
use polars::prelude::Engine;
use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;
#[cfg(not(feature = "pyo3"))]
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
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

/// The engine used for query result collection. `MAPLIB_POLARS_ENGINE=in-memory` selects the
/// in-memory engine, anything else the streaming engine.
fn query_engine() -> Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    *ENGINE.get_or_init(
        || match std::env::var("MAPLIB_POLARS_ENGINE").as_deref() {
            Ok("in-memory") | Ok("in_memory") => Engine::InMemory,
            _ => Engine::Streaming,
        },
    )
}

/// Installs Polars' SIGINT hook, which lets running queries abort on Ctrl-C. The previous
/// (Python) handler is chained, so Python still raises KeyboardInterrupt afterwards.
pub fn register_interrupt_handler() {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(register_polars_abort_mechanism);
}

/// collect a Polars LazyFrame in a way that allows a KeyboardInterrupt to cancel the search
pub fn pl_interruptable_collect(lf: LazyFrame) -> Result<DataFrame, InterruptableCollectError> {
    match query_engine() {
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

pub fn pl_vec_interruptable_collect(
    lfs: Vec<LazyFrame>,
    #[cfg(feature = "pyo3")] py: Python<'_>,
) -> Result<Vec<DataFrame>, InterruptableCollectError> {
    // println!("Entering an interruptable collect");

    let mut futures = HashMap::new();
    for (i, lf) in lfs.into_iter().enumerate() {
        let future = lf.collect_concurrently()?;
        futures.insert(i, future);
    }

    #[cfg(feature = "pyo3")]
    {
        // TODO: https://github.com/pola-rs/polars/issues/22513
        //                     min  med  avg  95%   99%   99.9%
        // check_signals takes 20 - 30 - 85 - 261 - 691 - 908
        // It's better to give it some time rather than waste it on checking signals
        let mut sleeptime = Duration::from_nanos(15);
        let mut dfs = HashMap::new();
        let mut ok_count = 0;
        loop {
            sleep(sleeptime);
            for (i, future) in &futures {
                if let std::collections::hash_map::Entry::Vacant(e) = dfs.entry(i) {
                    match future.fetch() {
                        None => {
                            if py.check_signals().is_err() {
                                // Polars has some kind of race condition and panics as it tries to tx on our dropped rx
                                // We've already decided to bail out, so waiting a few ms for rayon to clean up or whatever should be fine
                                // TODO: https://github.com/pola-rs/polars/issues/22515
                                future.cancel();
                                sleep(Duration::from_millis(50));
                                return Err(InterruptableCollectError::Interrupted);
                            }
                            // Max delay is twice the time the query actually took
                        }
                        Some(dfr) => {
                            e.insert(dfr?);
                            ok_count += 1;
                        }
                    }
                }
            }
            if ok_count == futures.len() {
                break;
            }
            sleeptime = std::cmp::min(sleeptime * 2, Duration::from_millis(50));
        }
        let results: Vec<_> = (0..dfs.len()).map(|i| dfs.remove(&i).unwrap()).collect();
        Ok(results)
    }

    #[cfg(not(feature = "pyo3"))]
    {
        let dfs: Result<Vec<_>, _> = futures
            .into_par_iter()
            .map(|(_, x)| x.fetch_blocking())
            .collect();
        Ok(dfs?)
    }
}
