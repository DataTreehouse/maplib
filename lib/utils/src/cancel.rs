//! Cooperative cancellation of queries: a deadline (a timeout) and a cancel flag.
//!
//! A [CancelToken] is made current on the thread that evaluates a query with [with_token].
//! Evaluation checks it between operators ([check]), in loops such as path search, while waiting
//! for Polars collects, and once per batch after each join (see [cancel_check_node]), and stops
//! with [Cancelled] when the deadline has passed, the token was cancelled, e.g. because the
//! client of a server went away, or the process uses more memory than queries may
//! ([crate::memory::query_memory_limit]).
//!
//! A token can also have a memory limit of its own: the query stops when the memory allocated by
//! maplib has grown by more than the limit since the query started (see
//! [CancelToken::with_memory_limit] and [set_default_query_memory_limit]). A timeout alone does not
//! bound memory, since a join can produce gigabytes of rows per second. The memory is that counted
//! by the global allocator (see [crate::memory::memory_usage]), which is shared by all queries of
//! the process, so with queries running at the same time the limit is approximate. Without that
//! allocator it never trips.
//!
//! Polars' own abort mechanism (used for KeyboardInterrupt) aborts every query in the process,
//! so it is not used here: a timeout stops only its own query.

use polars::prelude::{DataFrame, LazyFrame, OptFlags, PolarsError, PolarsResult};
use std::cell::RefCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const RUNNING: u8 = 0;
const TIMED_OUT: u8 = 1;
const CANCELLED: u8 = 2;
const MEMORY_LIMIT: u8 = 3;
const QUERY_MEMORY_LIMIT: u8 = 4;

/// The memory limit of queries whose token does not set one, in bytes, 0 for none.
static DEFAULT_QUERY_MEMORY_LIMIT: AtomicU64 = AtomicU64::new(0);

/// Sets the memory limit of every query that does not set its own (see
/// [CancelToken::with_memory_limit]), e.g. the queries of a served model. None removes it.
/// Applies to tokens made afterwards.
pub fn set_default_query_memory_limit(bytes: Option<u64>) {
    DEFAULT_QUERY_MEMORY_LIMIT.store(bytes.unwrap_or(0), Ordering::Relaxed);
}

/// The memory limit of queries that do not set their own, in bytes.
pub fn default_query_memory_limit() -> Option<u64> {
    match DEFAULT_QUERY_MEMORY_LIMIT.load(Ordering::Relaxed) {
        0 => None,
        b => Some(b),
    }
}

/// The message of the Polars error raised by a cancellation check inside a Polars plan.
pub const CANCELLED_POLARS_MESSAGE: &str = "maplib query cancelled";

/// Why a query stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cancelled {
    /// The query ran longer than its timeout.
    Timeout(Duration),
    /// The query was cancelled, e.g. because the client disconnected.
    Cancelled,
    /// The process used more than this many bytes, the memory allowed for queries.
    MemoryLimit(u64),
    /// The memory allocated while the query ran grew by more than this many bytes, the memory
    /// limit of the query (see [CancelToken::with_memory_limit]).
    QueryMemoryLimit(u64),
}

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cancelled::Timeout(d) => write!(f, "The query timed out after {}", fmt_duration(*d)),
            Cancelled::Cancelled => write!(f, "The query was cancelled"),
            Cancelled::MemoryLimit(limit) => write!(
                f,
                "The query was stopped as the process used more than {} MB, 90% of the memory \
                 limit abort_above_mb",
                limit / 1_000_000
            ),
            Cancelled::QueryMemoryLimit(limit) => write!(
                f,
                "The query was stopped as it used more than its memory limit of {} MB",
                limit.div_ceil(1_000_000)
            ),
        }
    }
}

impl std::error::Error for Cancelled {}

fn fmt_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s.fract() == 0.0 {
        format!("{s:.0} s")
    } else {
        format!("{s:.3} s")
    }
}

#[derive(Debug)]
struct Inner {
    state: AtomicU8,
    /// Set when the query has a join that may give many rows, see [note_large_join].
    large_join: AtomicBool,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    /// The most bytes the memory allocated by the process may grow by while the query runs, see
    /// [CancelToken::with_memory_limit].
    memory_limit: Option<u64>,
    /// The bytes allocated by the process when the token was made.
    memory_start: u64,
}

/// A deadline and a cancel flag shared by everything that evaluates one query. Cloning gives
/// another handle to the same token.
#[derive(Debug, Clone)]
pub struct CancelToken(Arc<Inner>);

impl CancelToken {
    /// A token that times out `timeout` from now, or never with None. It has the default memory
    /// limit, see [set_default_query_memory_limit].
    pub fn new(timeout: Option<Duration>) -> CancelToken {
        CancelToken::with_deadline(timeout, timeout.map(|t| Instant::now() + t))
    }

    /// A token that times out at `deadline`. `timeout` is the duration reported in the error.
    /// It has the default memory limit, see [set_default_query_memory_limit].
    pub fn with_deadline(timeout: Option<Duration>, deadline: Option<Instant>) -> CancelToken {
        CancelToken::with_memory_limit(timeout, deadline, default_query_memory_limit())
    }

    /// A token that times out at `deadline`, and stops the query when the memory allocated by
    /// maplib grows by more than `memory_limit` bytes from now. None means no memory limit of
    /// its own (the process-wide [crate::memory::query_memory_limit] still applies).
    pub fn with_memory_limit(
        timeout: Option<Duration>,
        deadline: Option<Instant>,
        memory_limit: Option<u64>,
    ) -> CancelToken {
        CancelToken(Arc::new(Inner {
            state: AtomicU8::new(RUNNING),
            large_join: AtomicBool::new(false),
            timeout,
            deadline,
            memory_limit,
            memory_start: memory_limit.map_or(0, |_| crate::memory::memory_usage()),
        }))
    }

    /// The memory limit of the query in bytes, see [CancelToken::with_memory_limit].
    pub fn memory_limit(&self) -> Option<u64> {
        self.0.memory_limit
    }

    /// Asks the query to stop as soon as possible.
    pub fn cancel(&self) {
        let _ = self.0.state.compare_exchange(
            RUNNING,
            CANCELLED,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.0.deadline
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.0.timeout
    }

    /// Why the query must stop, or None if it may go on. Checks the deadline and the memory
    /// limits, which is cheap (atomic loads).
    pub fn stopped(&self) -> Option<Cancelled> {
        match self.0.state.load(Ordering::Relaxed) {
            RUNNING => {
                if let Some(deadline) = self.0.deadline {
                    if Instant::now() >= deadline {
                        let _ = self.0.state.compare_exchange(
                            RUNNING,
                            TIMED_OUT,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                        return self.stopped();
                    }
                }
                if let Some(limit) = crate::memory::query_memory_limit() {
                    if crate::memory::memory_usage() > limit {
                        let _ = self.0.state.compare_exchange(
                            RUNNING,
                            MEMORY_LIMIT,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                        return self.stopped();
                    }
                }
                if let Some(limit) = self.0.memory_limit {
                    let grown = crate::memory::memory_usage().saturating_sub(self.0.memory_start);
                    if grown > limit {
                        let _ = self.0.state.compare_exchange(
                            RUNNING,
                            QUERY_MEMORY_LIMIT,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        );
                        return self.stopped();
                    }
                }
                None
            }
            TIMED_OUT => Some(Cancelled::Timeout(self.0.timeout.unwrap_or_default())),
            MEMORY_LIMIT => Some(Cancelled::MemoryLimit(
                crate::memory::query_memory_limit().unwrap_or_default(),
            )),
            QUERY_MEMORY_LIMIT => Some(Cancelled::QueryMemoryLimit(
                self.0.memory_limit.unwrap_or_default(),
            )),
            _ => Some(Cancelled::Cancelled),
        }
    }

    /// Err when the query must stop.
    pub fn check(&self) -> Result<(), Cancelled> {
        match self.stopped() {
            Some(c) => Err(c),
            None => Ok(()),
        }
    }
}

thread_local! {
    static CURRENT: RefCell<Option<CancelToken>> = const { RefCell::new(None) };
}

/// Runs `f` with `token` as the current token of this thread, restoring the previous one after.
/// With None, `f` runs with the token that is already current, if any.
pub fn with_token<R>(token: Option<CancelToken>, f: impl FnOnce() -> R) -> R {
    let Some(token) = token else {
        return f();
    };
    struct Restore(Option<CancelToken>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let prev = self.0.take();
            CURRENT.with(|c| *c.borrow_mut() = prev);
        }
    }
    let prev = CURRENT.with(|c| c.borrow_mut().replace(token));
    let _restore = Restore(prev);
    f()
}

/// Runs `f` without a current token, e.g. for writes that must not stop halfway.
pub fn suspended<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(Option<CancelToken>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let prev = self.0.take();
            CURRENT.with(|c| *c.borrow_mut() = prev);
        }
    }
    let prev = CURRENT.with(|c| c.borrow_mut().take());
    let _restore = Restore(prev);
    f()
}

/// The token of the query evaluated on this thread, if it has one. Work moved to other threads
/// (e.g. rayon) takes a clone of it.
pub fn current() -> Option<CancelToken> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Err when the query evaluated on this thread must stop.
pub fn check() -> Result<(), Cancelled> {
    CURRENT.with(|c| match &*c.borrow() {
        Some(t) => t.check(),
        None => Ok(()),
    })
}

/// Why the query evaluated on this thread stopped, if it did.
pub fn stopped() -> Option<Cancelled> {
    CURRENT.with(|c| c.borrow().as_ref().and_then(|t| t.stopped()))
}

/// Records that the query evaluated on this thread has a join that may give many rows. Such a
/// query is collected with the streaming engine even when its result is estimated to be small
/// (e.g. a COUNT), since the in-memory engine can not stop a join that is running.
pub fn note_large_join() {
    CURRENT.with(|c| {
        if let Some(t) = &*c.borrow() {
            t.0.large_join.store(true, Ordering::Relaxed);
        }
    })
}

/// Whether the query evaluated on this thread can be stopped and has a large join.
pub fn has_large_join() -> bool {
    CURRENT.with(|c| {
        c.borrow()
            .as_ref()
            .is_some_and(|t| t.0.large_join.load(Ordering::Relaxed))
    })
}

/// Adds a check of the current token to `lf`, run on every batch that passes, so a long join
/// stops soon after its query was cancelled, timed out or went over the memory allowed for
/// queries, also on the streaming engine. The plan is unchanged when this thread has no token.
pub fn cancel_check_node(lf: LazyFrame) -> LazyFrame {
    let Some(token) = current() else {
        return lf;
    };
    lf.map(
        move |df: DataFrame| -> PolarsResult<DataFrame> {
            match token.stopped() {
                None => Ok(df),
                Some(_) => Err(PolarsError::ComputeError(CANCELLED_POLARS_MESSAGE.into())),
            }
        },
        OptFlags::PREDICATE_PUSHDOWN | OptFlags::PROJECTION_PUSHDOWN | OptFlags::STREAMING,
        None,
        Some("maplib cancel check"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_out() {
        let t = CancelToken::new(Some(Duration::from_millis(10)));
        assert!(t.check().is_ok());
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            t.check(),
            Err(Cancelled::Timeout(Duration::from_millis(10)))
        );
        // A timed out token stays timed out when it is cancelled.
        t.cancel();
        assert!(matches!(t.stopped(), Some(Cancelled::Timeout(_))));
    }

    #[test]
    fn cancel_and_current() {
        let t = CancelToken::new(None);
        assert!(check().is_ok());
        with_token(Some(t.clone()), || {
            assert!(check().is_ok());
            t.cancel();
            assert_eq!(check(), Err(Cancelled::Cancelled));
        });
        assert!(current().is_none());
    }
}
