use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
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

/// collect a Polars LazyFrame in a way that allows a KeyboardInterrupt to cancel the search
pub fn pl_interruptable_collect(
    lf: LazyFrame,
    #[cfg(feature = "pyo3")] py: Python<'_>,
) -> Result<DataFrame, InterruptableCollectError> {
    // println!("Entering an interruptable collect");

    let future = lf.collect_concurrently()?;

    #[cfg(feature = "pyo3")]
    {
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
                        return Err(InterruptableCollectError::Interrupted);
                    }
                    // Max delay is twice the time the query actually took
                    sleeptime = std::cmp::min(sleeptime * 2, Duration::from_millis(50));
                }
                Some(dfr) => break Ok(dfr?),
            }
        }
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
