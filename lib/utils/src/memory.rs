//! The memory budget of the Polars streaming engine.
//!
//! When the binary uses [`polars_ooc::Allocator`] as its global allocator, every allocation is
//! counted, and the streaming engine spills buffered data (join build sides, group-by and sort
//! state, collected results) to disk when the process goes over the budget. Without that
//! allocator the count stays at zero and nothing is ever spilled.
//!
//! The budget defaults to 80% of the system memory (respecting cgroup limits). It counts all
//! memory of the process, including the triplestore itself, which cannot be spilled.
//!
//! Polars reads these settings from `POLARS_OOC_*` environment variables. The functions here set
//! those variables and reload them, so the settings apply to queries started afterwards.

use std::path::PathBuf;

const MEMORY_BUDGET_MB: &str = "POLARS_OOC_MEMORY_BUDGET_MB";
const MEMORY_BUDGET_FRACTION: &str = "POLARS_OOC_MEMORY_BUDGET_FRACTION";
const DISK_BUDGET_MB: &str = "POLARS_OOC_DISK_BUDGET_MB";
const SPILL_DIR: &str = "POLARS_OOC_SPILL_DIR";
const ABORT_THRESHOLD_MB: &str = "POLARS_OOMKILL_THRESHOLD_MB";
const DRIFT_THRESHOLD: &str = "POLARS_OOC_DRIFT_THRESHOLD";

/// The smallest and largest drift threshold set for `abort_above_mb`, see [drift_threshold].
const MIN_DRIFT_BYTES: u64 = 64 * 1024;
const MAX_DRIFT_BYTES: u64 = 4 * 1024 * 1024;

/// Whether maplib set POLARS_OOC_DRIFT_THRESHOLD, so that it may change or remove it again. A
/// value set by the user before is left as it is.
static DRIFT_SET_BY_MAPLIB: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Limits on the memory used by queries. `None` restores the Polars default of a field.
#[derive(Debug, Clone, Default)]
pub struct MemoryLimits {
    /// Spill to disk when the process uses more than this many megabytes (10^6 bytes).
    /// When set, it replaces the default of 80% of system memory.
    pub max_memory_mb: Option<u64>,
    /// The directory spill files are written to. Defaults to the system temporary directory.
    /// Polars fixes it at the first spill, so it must be set before any query runs.
    pub spill_dir: Option<PathBuf>,
    /// The most megabytes to spill to disk. Defaults to 64 GB.
    pub max_spill_mb: Option<u64>,
    /// Abort the process when it uses more than this many megabytes, even after spilling.
    /// Off by default. This is a hard stop that beats the OS OOM killer to it, not an error.
    pub abort_above_mb: Option<u64>,
}

/// Applies `limits` to the Polars streaming engine.
pub fn set_memory_limits(limits: &MemoryLimits) {
    set_var(MEMORY_BUDGET_MB, limits.max_memory_mb.map(|x| x.to_string()));
    // Polars uses the smaller of the MB budget and the fraction of system memory, so an explicit
    // budget above 80% of system memory would otherwise be capped.
    set_var(
        MEMORY_BUDGET_FRACTION,
        limits.max_memory_mb.map(|_| "1.0".to_string()),
    );
    set_var(DISK_BUDGET_MB, limits.max_spill_mb.map(|x| x.to_string()));
    set_var(
        SPILL_DIR,
        limits
            .spill_dir
            .as_ref()
            .map(|x| x.to_string_lossy().into_owned()),
    );
    set_var(
        ABORT_THRESHOLD_MB,
        limits.abort_above_mb.map(|x| x.to_string()),
    );
    let config = polars_config::config();
    let by_user = std::env::var_os(DRIFT_THRESHOLD).is_some()
        && !DRIFT_SET_BY_MAPLIB.load(std::sync::atomic::Ordering::Relaxed);
    if !by_user {
        let drift = limits
            .abort_above_mb
            .map(|mb| drift_threshold(mb * 1_000_000, config.max_threads()));
        DRIFT_SET_BY_MAPLIB.store(drift.is_some(), std::sync::atomic::Ordering::Relaxed);
        set_var(DRIFT_THRESHOLD, drift.map(|x| x.to_string()));
    }
    for var in [
        MEMORY_BUDGET_MB,
        MEMORY_BUDGET_FRACTION,
        DISK_BUDGET_MB,
        SPILL_DIR,
        ABORT_THRESHOLD_MB,
        DRIFT_THRESHOLD,
    ] {
        config.reload_env_var(var);
    }
}

/// The memory each thread may allocate between two checks of a query's memory, see
/// [abort_margin]: a batch of a join in flight and what the thread has not counted yet.
const BYTES_IN_FLIGHT_PER_THREAD: u64 = 4 * 1024 * 1024;

/// The bytes below `abort_above` at which queries are stopped ([query_memory_limit]), with
/// `threads` Polars threads (and a few more for maplib's own). Queries are checked for every batch
/// a join gives, but every thread may have a batch in flight and memory it has not counted yet
/// (see [drift_threshold]), so with many threads a query went past the abort threshold before it
/// was stopped: 64 threads went over a margin of 10% of 500 MB. The margin is 10%, or what the
/// threads may allocate between two checks when that is more, but at most half the threshold.
fn abort_margin(abort_above: u64, threads: usize) -> u64 {
    let in_flight = (threads as u64 + 4).saturating_mul(BYTES_IN_FLIGHT_PER_THREAD);
    (abort_above / 10).max(in_flight).min(abort_above / 2)
}

/// The drift threshold for a process aborted above `abort_above` bytes with `threads` Polars
/// threads. Each thread counts what it allocates on its own until it has allocated or freed this
/// many bytes, and only then adds it to the memory of the process that queries are stopped on
/// ([query_memory_limit]) and that Polars aborts on. At the default of 4 MB, 64 threads could
/// allocate 256 MB the process did not count yet. With this threshold they hold at most a quarter
/// of the margin between the two limits ([abort_margin]).
fn drift_threshold(abort_above: u64, threads: usize) -> u64 {
    (abort_margin(abort_above, threads) / 4 / (threads as u64 + 4))
        .clamp(MIN_DRIFT_BYTES, MAX_DRIFT_BYTES)
}

fn set_var(var: &str, value: Option<String>) {
    match value {
        Some(value) => std::env::set_var(var, value),
        None => std::env::remove_var(var),
    }
}

/// The estimated bytes allocated by the process. Zero unless the global allocator is
/// [`polars_ooc::Allocator`].
pub fn memory_usage() -> u64 {
    polars_ooc::estimate_memory_usage()
}

/// The memory budget in bytes above which the streaming engine spills to disk.
pub fn memory_budget() -> u64 {
    polars_config::config().ooc_memory_budget_bytes()
}

/// The memory in bytes above which a query is stopped with an error, so that a query that grows
/// too large fails on its own instead of taking the process down: 90% of the threshold at which
/// Polars aborts the process (`abort_above_mb`), or less with many threads, see [abort_margin].
/// None when no such threshold is set.
pub fn query_memory_limit() -> Option<u64> {
    let abort_above = polars_config::get_oomkill_threshold_bytes();
    if abort_above > i64::MAX as u64 {
        None
    } else {
        Some(abort_above - abort_margin(abort_above, polars_config::config().max_threads()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_margin_grows_with_the_threads() {
        // 10% with few threads or a high threshold.
        assert_eq!(abort_margin(16_000_000_000, 16), 1_600_000_000);
        assert_eq!(abort_margin(1_000_000_000, 4), 100_000_000);
        // 500 MB with 15 threads: 19 batches in flight of 4 MiB are more than 10%.
        assert_eq!(abort_margin(500_000_000, 15), 19 * BYTES_IN_FLIGHT_PER_THREAD);
        // With 64 threads it would be more than half, so a query may still use half.
        assert_eq!(abort_margin(500_000_000, 64), 250_000_000);
    }

    #[test]
    fn drift_threshold_keeps_the_threads_within_the_margin() {
        // 500 MB with 64 threads: all threads together may hold a quarter of the margin.
        let d = drift_threshold(500_000_000, 64);
        assert!(d * 68 <= abort_margin(500_000_000, 64) / 4);
        assert!(d >= MIN_DRIFT_BYTES);
        // Few threads and a high threshold keep Polars' default.
        assert_eq!(drift_threshold(100_000_000_000, 4), MAX_DRIFT_BYTES);
        // A threshold far too small for the threads still counts in steps of 64 KB.
        assert_eq!(drift_threshold(10_000_000, 256), MIN_DRIFT_BYTES);
    }
}
