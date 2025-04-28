use std::{thread::sleep, time::Duration};

use polars::{error::PolarsError, frame::DataFrame, prelude::LazyFrame};
use thiserror::Error;

#[cfg(feature = "pyo3")]
use pyo3::Python;

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

    let future = lf.collect_concurrently().unwrap();

    #[cfg(feature = "pyo3")]
    {
        //                     min  med  avg  95%   99%   99.9%
        // check_signals takes 20 - 30 - 85 - 261 - 691 - 908
        // It's better to give it some time rather than waste it on checking signals
        let mut sleeptime = Duration::from_nanos(15);
        loop {
            sleep(sleeptime);
            match future.fetch() {
                None => {
                    if py.check_signals().is_err() {
                        future.cancel();
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
