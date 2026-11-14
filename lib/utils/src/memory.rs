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
    for var in [
        MEMORY_BUDGET_MB,
        MEMORY_BUDGET_FRACTION,
        DISK_BUDGET_MB,
        SPILL_DIR,
        ABORT_THRESHOLD_MB,
    ] {
        config.reload_env_var(var);
    }
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
