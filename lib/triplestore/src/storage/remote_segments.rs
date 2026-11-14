//! Triple table files of disk storage that are kept in object storage, with a local copy as a
//! cache.
//!
//! When a model is deserialized from a serialization folder in object storage into disk
//! storage, its triple tables are not downloaded. Each table is downloaded to the triples folder
//! the first time it is read, and read locally from then on. With a cache limit (see
//! [`set_cache_limit`]), the least recently read downloaded tables are removed from the triples
//! folder when the downloaded tables take more space than the limit, and downloaded again when
//! they are next read. Tables that were read in the last [`EVICTION_GRACE`] are never removed,
//! as queries that are running may still read them, so the limit can be exceeded for a while.
//!
//! Triple tables made locally (by inserts and compaction) are not in object storage, so they
//! are never removed from the cache: they stay until the model is serialized and deserialized
//! again.

use file_io::remote::RemoteLocation;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Downloaded tables read this recently are never removed from the cache, unless changed with
/// the environment variable [`EVICTION_GRACE_ENV`] (in seconds).
pub const EVICTION_GRACE: Duration = Duration::from_secs(300);
pub const EVICTION_GRACE_ENV: &str = "MAPLIB_REMOTE_CACHE_GRACE_SECONDS";

fn eviction_grace() -> Duration {
    std::env::var(EVICTION_GRACE_ENV)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|s| s.is_finite() && *s >= 0.0)
        .map(Duration::from_secs_f64)
        .unwrap_or(EVICTION_GRACE)
}

/// The environment variable with the default cache limit in bytes.
pub const CACHE_LIMIT_ENV: &str = "MAPLIB_REMOTE_CACHE_BYTES";

/// 0 is no limit.
static CACHE_LIMIT: LazyLock<AtomicU64> = LazyLock::new(|| {
    AtomicU64::new(
        std::env::var(CACHE_LIMIT_ENV)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0),
    )
});

static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Sets the largest number of bytes of downloaded triple tables kept in the triples folders of
/// the process, None for no limit (the default, or the value of `MAPLIB_REMOTE_CACHE_BYTES`).
pub fn set_cache_limit(bytes: Option<u64>) {
    CACHE_LIMIT.store(bytes.unwrap_or(0), Ordering::SeqCst);
}

pub fn cache_limit() -> Option<u64> {
    match CACHE_LIMIT.load(Ordering::SeqCst) {
        0 => None,
        b => Some(b),
    }
}

struct CacheEntry {
    segment: Weak<RemoteSegment>,
}

#[derive(Default)]
struct Cache {
    /// Downloaded segments by their local path.
    entries: HashMap<PathBuf, CacheEntry>,
}

static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(|| Mutex::new(Cache::default()));

/// A triple table file in object storage, downloaded to `path` when it is read.
pub struct RemoteSegment {
    location: RemoteLocation,
    path: PathBuf,
    /// The size in bytes of the local copy, 0 when there is none. Guards downloads.
    local_size: Mutex<u64>,
    /// Milliseconds since START at the last read.
    last_read: AtomicU64,
}

impl RemoteSegment {
    pub fn new(location: RemoteLocation, path: PathBuf) -> Arc<RemoteSegment> {
        Arc::new(RemoteSegment {
            location,
            path,
            local_size: Mutex::new(0),
            last_read: AtomicU64::new(0),
        })
    }

    pub fn location(&self) -> &RemoteLocation {
        &self.location
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_local(&self) -> bool {
        *self.local_size.lock().unwrap() > 0
    }

    fn touch(&self) {
        self.last_read
            .store(START.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    fn idle(&self) -> Duration {
        let now = START.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_read.load(Ordering::Relaxed)))
    }

    /// Makes sure the table is in the local file, downloading it if needed, and returns its
    /// path. Called before every read.
    pub fn ensure_local(self: &Arc<Self>) -> std::io::Result<&Path> {
        self.touch();
        let mut size = self.local_size.lock().unwrap();
        if *size > 0 && self.path.exists() {
            return Ok(&self.path);
        }
        let start = Instant::now();
        let mut tmp = self.path.as_os_str().to_owned();
        tmp.push(".download");
        let tmp = PathBuf::from(tmp);
        let res = self.location.download_to(&tmp).and_then(|n| {
            std::fs::rename(&tmp, &self.path)
                .map_err(|e| file_io::remote::RemoteError::Io(self.path.display().to_string(), e))?;
            Ok(n)
        });
        let downloaded = match res {
            Ok(n) => n,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e.into());
            }
        };
        *size = downloaded.max(1);
        drop(size);
        debug!(
            "Downloaded {} ({} bytes) in {:.3} s",
            self.location,
            downloaded,
            start.elapsed().as_secs_f64()
        );
        CACHE.lock().unwrap().entries.insert(
            self.path.clone(),
            CacheEntry {
                segment: Arc::downgrade(self),
            },
        );
        evict_if_needed(&self.path);
        Ok(&self.path)
    }

    /// Removes the local copy, if it was not read in the last `grace`. Returns the bytes freed.
    fn evict(&self, grace: Duration) -> u64 {
        if self.idle() < grace {
            return 0;
        }
        let Ok(mut size) = self.local_size.try_lock() else {
            return 0;
        };
        if *size == 0 {
            return 0;
        }
        if let Err(e) = std::fs::remove_file(&self.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Could not remove cached triples file {:?}: {}", self.path, e);
                return 0;
            }
        }
        let freed = *size;
        *size = 0;
        debug!("Removed the cached copy of {}", self.location);
        freed
    }
}

impl Drop for RemoteSegment {
    fn drop(&mut self) {
        if let Ok(mut cache) = CACHE.lock() {
            cache.entries.remove(&self.path);
        }
        if *self.local_size.get_mut().unwrap() > 0 {
            if let Err(e) = std::fs::remove_file(&self.path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Could not remove cached triples file {:?}: {}", self.path, e);
                }
            }
        }
    }
}

/// The total size of the downloaded tables in the cache.
pub fn cached_bytes() -> u64 {
    let cache = CACHE.lock().unwrap();
    cache
        .entries
        .values()
        .filter_map(|e| e.segment.upgrade())
        .map(|s| s.local_size.try_lock().map(|g| *g).unwrap_or(0))
        .sum()
}

fn evict_if_needed(just_downloaded: &Path) {
    evict_to_limit(just_downloaded, eviction_grace());
}

/// Removes the least recently read downloaded tables, other than `keep`, that were not read in
/// the last `grace`, until the cache is within its limit.
fn evict_to_limit(keep: &Path, grace: Duration) {
    let Some(limit) = cache_limit() else {
        return;
    };
    let mut segments: Vec<Arc<RemoteSegment>> = {
        let mut cache = CACHE.lock().unwrap();
        cache.entries.retain(|_, e| e.segment.strong_count() > 0);
        cache
            .entries
            .values()
            .filter_map(|e| e.segment.upgrade())
            .collect()
    };
    let mut total: u64 = segments
        .iter()
        .map(|s| s.local_size.try_lock().map(|g| *g).unwrap_or(0))
        .sum();
    if total <= limit {
        return;
    }
    // Least recently read first.
    segments.sort_by_key(|s| std::cmp::Reverse(s.idle()));
    for s in segments {
        if total <= limit {
            break;
        }
        if s.path == keep {
            continue;
        }
        total = total.saturating_sub(s.evict(grace));
    }
    if total > limit {
        debug!(
            "Cached triple tables take {total} bytes, more than the limit of {limit}, as the \
             rest were read recently"
        );
    }
}

