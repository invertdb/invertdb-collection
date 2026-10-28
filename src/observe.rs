//! A collection's telemetry: counters kept as it works, its [`Stats`], and the `invertdb.collection.*` metrics (the
//! `opentelemetry` API: no-ops until the program installs a meter provider).
//!
//! Each open collection registers its counters; the observable instruments read every registered collection when
//! metrics are collected, with the collection's name as the `invertdb.collection` attribute. A dropped collection's
//! counters drop out.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use larry::Store;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use serde_json::{Value, json};

/// What a collection holds and has done, for `/_stats`.
#[derive(Clone, Debug, PartialEq)]
pub struct Stats {
    pub name: String,
    /// Sources in the index, deleted ones included.
    pub sources: u32,
    pub deleted: u32,
    /// Logged operations not yet committed.
    pub pending: u64,
    pub commits: u64,
    /// When the last commit finished, and how long it took.
    pub last_commit: Option<(SystemTime, Duration)>,
    /// Bytes on disk, everything included (the log too).
    pub disk_bytes: u64,
    pub log_bytes: u64,
}

impl Stats {
    pub fn live(&self) -> u32 {
        self.sources - self.deleted
    }

    pub fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "sources": self.sources,
            "live": self.live(),
            "deleted": self.deleted,
            "pending": self.pending,
            "commits": self.commits,
            "last_commit": self.last_commit.map(|(at, took)| json!({
                "at_unix_seconds": at.duration_since(UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64()),
                "seconds": took.as_secs_f64(),
            })),
            "disk_bytes": self.disk_bytes,
            "log_bytes": self.log_bytes,
        })
    }
}

/// Counters a collection keeps up to date, which metrics collection reads without taking the collection's locks.
pub(crate) struct Counters {
    name: String,
    files: Store,
    log: Store,
    pub(crate) sources: AtomicU32,
    pub(crate) deleted: AtomicU32,
    pub(crate) pending: AtomicU64,
    commits: AtomicU64,
    /// The last commit's end (nanoseconds since the epoch) and duration (nanoseconds); 0 before the first.
    last_at: AtomicU64,
    last_took: AtomicU64,
}

impl Counters {
    /// Counters for the collection in `files`, registered for the metrics; named after the store's directory.
    pub(crate) fn register(files: &Store) -> std::io::Result<Arc<Counters>> {
        let log = files.path("log")?;
        let name = log.parent().and_then(|p| p.file_name()).map_or_else(String::new, |n| n.to_string_lossy().into_owned());
        let counters = Arc::new(Counters {
            name,
            files: files.clone(),
            log: Store::open(log)?,
            sources: AtomicU32::new(0),
            deleted: AtomicU32::new(0),
            pending: AtomicU64::new(0),
            commits: AtomicU64::new(0),
            last_at: AtomicU64::new(0),
            last_took: AtomicU64::new(0),
        });
        instruments(); // the observable instruments, once
        let mut registry = registry().lock().unwrap();
        registry.retain(|c| c.strong_count() > 0);
        registry.push(Arc::downgrade(&counters));
        Ok(counters)
    }

    pub(crate) fn attributes(&self) -> [KeyValue; 1] {
        [KeyValue::new("invertdb.collection", self.name.clone())]
    }

    pub(crate) fn committed(&self, took: Duration, added: u32, deleted: u32) {
        let at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        self.last_at.store(at.as_nanos() as u64, Ordering::Relaxed);
        self.last_took.store(took.as_nanos() as u64, Ordering::Relaxed);
        self.commits.fetch_add(1, Ordering::Relaxed);
        let i = instruments();
        let attributes = self.attributes();
        i.commit.record(took.as_secs_f64(), &attributes);
        if added > 0 {
            i.added.add(added as u64, &attributes);
        }
        if deleted > 0 {
            i.deleted.add(deleted as u64, &attributes);
        }
    }

    pub(crate) fn stats(&self) -> Stats {
        let at = self.last_at.load(Ordering::Relaxed);
        Stats {
            name: self.name.clone(),
            sources: self.sources.load(Ordering::Relaxed),
            deleted: self.deleted.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
            last_commit: (at > 0).then(|| (UNIX_EPOCH + Duration::from_nanos(at), Duration::from_nanos(self.last_took.load(Ordering::Relaxed)))),
            disk_bytes: self.files.size(),
            log_bytes: self.log.size(),
        }
    }
}

fn registry() -> &'static Mutex<Vec<Weak<Counters>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<Counters>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Every open collection's counters.
fn open() -> Vec<Arc<Counters>> {
    registry().lock().unwrap().iter().filter_map(Weak::upgrade).collect()
}

pub(crate) struct Instruments {
    commit: Histogram<f64>,
    added: Counter<u64>,
    deleted: Counter<u64>,
    pub(crate) ops: Counter<u64>,
}

/// Made on first use (after a service installs its meter provider).
pub(crate) fn instruments() -> &'static Instruments {
    static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();
    INSTRUMENTS.get_or_init(|| {
        let meter = global::meter("invertdb-collection");
        let gauge = |name: &'static str, unit: &'static str, description: &'static str, read: fn(&Counters) -> u64| {
            meter
                .u64_observable_gauge(name)
                .with_unit(unit)
                .with_description(description)
                .with_callback(move |o| {
                    for c in open() {
                        o.observe(read(&c), &c.attributes());
                    }
                })
                .build();
        };
        gauge("invertdb.collection.sources", "{source}", "Live sources in a collection.", |c| (c.sources.load(Ordering::Relaxed) - c.deleted.load(Ordering::Relaxed)) as u64);
        gauge("invertdb.collection.deleted", "{source}", "Deleted sources a collection still holds (not yet reclaimed).", |c| c.deleted.load(Ordering::Relaxed) as u64);
        gauge("invertdb.collection.pending", "{operation}", "Operations logged but not yet committed.", |c| c.pending.load(Ordering::Relaxed));
        gauge("invertdb.collection.disk", "By", "Bytes a collection takes on disk, its log included.", |c| c.files.size());
        gauge("invertdb.collection.log", "By", "Bytes in a collection's write-ahead log.", |c| c.log.size());
        Instruments {
            commit: meter
                .f64_histogram("invertdb.collection.commit.duration")
                .with_unit("s")
                .with_description("Time to commit a collection: apply, index commit, key table, publish, log reset.")
                .with_boundaries(vec![0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0])
                .build(),
            added: meter.u64_counter("invertdb.collection.added").with_unit("{source}").with_description("Sources committed to collections.").build(),
            deleted: meter.u64_counter("invertdb.collection.removed").with_unit("{source}").with_description("Sources deleted or replaced by commits.").build(),
            ops: meter.u64_counter("invertdb.collection.operations").with_unit("{operation}").with_description("Operations logged, by kind (add, delete).").build(),
        }
    })
}
