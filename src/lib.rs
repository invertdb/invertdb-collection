//! The collection layer: what every invertdb service did for itself around an introvert index, once. A collection
//! takes writes (adds and deletes) into a write-ahead log, buffers them, and on [`Collection::commit`] applies them
//! and publishes a new [`Generation`]: the index's snapshot, the domain's side data, and which sources are deleted.
//! Readers take the current generation and see one consistent commit.
//!
//! A service supplies a [`Domain`]: how its records are logged, which key each has, and what a commit does with a
//! batch of them (write side data, such as vectors or coordinates, and add each record's units to the index).
//!
//! **Deletes and upserts.** A source is never removed from the index; a delete marks its ordinal deleted, and
//! queries skip deleted ordinals ([`Generation::is_live`]). With [`Keys::Unique`], adding a key that's already live
//! replaces it (the old source is marked deleted); with [`Keys::Multiple`], a key can name several sources, and a
//! delete removes them all. The key table (each key's live ordinals) and the deleted set are kept in a b-positive
//! file, `keys.db`.
//!
//! **Commit order and recovery.** A commit writes the domain's side data and adds the batch's records to the index
//! in log order (so each logged add's ordinal is known), commits the index (the commit point for the sources), then
//! records the batch's key changes and deleted ordinals, with how many sources and log operations they account for,
//! in one `keys.db` transaction; then it publishes and resets the log with those two counts. Opening replays the log,
//! taking each operation as one of: accounted for by the key table already (skipped); in the index but not the key
//! table, after a crash between the two commits (its key change is made now, in log order); or pending again.
//!
//! Storage, all in the collection's larry store: the index in the nested store `units`, the key table in `keys.db`,
//! the log in `log`, and whatever the domain keeps beside them.

use std::io;
use std::sync::{Arc, Mutex, RwLock};

use b_positive::{Db, WriteTxn};
use blammo::Log;
use introvert::{Codec, Index, Options, Snapshot, SourceSet};
use larry::Store;

/// How keys name sources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keys {
    /// A key names at most one live source: adding it again replaces the old one.
    Unique,
    /// A key can name several sources (adding it again adds another); a delete removes them all.
    Multiple,
}

/// What a service supplies: its records, and what a commit does with them.
pub trait Domain: Send + Sync + 'static {
    /// The index's source key.
    type Key: Codec + Send + Sync;
    /// The index's unit.
    type Unit: Codec + Send + Sync;
    /// What's added.
    type Record: Send + Sync;
    /// What's published with each commit beside the index (columns, caches); cloning is cheap (`Arc`s).
    type Side: Clone + Send + Sync;

    fn index_options(&self) -> Options;

    fn keys(&self) -> Keys {
        Keys::Unique
    }

    /// A record as a log entry.
    fn encode(&self, record: &Self::Record, out: &mut Vec<u8>);

    /// A logged record (None: damaged).
    fn decode(&self, bytes: &[u8]) -> Option<Self::Record>;

    fn key(&self, record: &Self::Record) -> Self::Key;

    /// Rejects a record before it's logged.
    fn check(&self, _record: &Self::Record) -> Result<(), String> {
        Ok(())
    }

    /// The side data as of the index's last commit, cutting off what a crash left past it.
    fn open(&self, snapshot: &Snapshot<Self::Key, Self::Unit>) -> io::Result<Self::Side>;

    /// Writes the batch's side data and adds each record to `index`, in order (so `batch[i]` becomes source
    /// `first + i`), and returns the side data with them. The collection commits the index after.
    fn apply(&self, batch: &[Self::Record], first: u32, side: &Self::Side, index: &Index<Self::Key, Self::Unit>) -> io::Result<Self::Side>;
}

/// What readers see: one commit.
pub struct Generation<D: Domain> {
    pub snapshot: Snapshot<D::Key, D::Unit>,
    pub side: D::Side,
    /// Deleted (and replaced) sources.
    pub deleted: Arc<SourceSet>,
    /// Sources in the index, deleted ones included.
    pub sources: u32,
}

impl<D: Domain> Generation<D> {
    pub fn is_live(&self, source: u32) -> bool {
        source < self.sources && !self.deleted.contains(source)
    }

    /// Sources not deleted.
    pub fn live(&self) -> u32 {
        self.sources - self.deleted.len() as u32
    }
}

enum Op<R, K> {
    Add(R),
    Delete(K),
}

const ADD: u8 = 0;
const DELETE: u8 = 1;
/// A log's checkpoint: the sources and the operations (logged since the collection began) as of its reset.
const CHECKPOINT: usize = 16;

// keys.db: `k` + key -> the key's live ordinals (u32 big-endian each); `d` + ordinal (big-endian) -> deleted; `m` ->
// the sources and log operations the table accounts for (u64 little-endian each)
const KEY: u8 = b'k';
const DELETED: u8 = b'd';
const META: &[u8] = b"m";

fn key_entry<K: Codec>(key: &K) -> Vec<u8> {
    let mut out = vec![KEY];
    key.encode(&mut out);
    out
}

fn ordinals(bytes: &[u8]) -> Vec<u32> {
    bytes.as_chunks::<4>().0.iter().map(|c| u32::from_be_bytes(*c)).collect()
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// One transaction's changes to the key table.
struct KeyChanges<'t, 'db> {
    txn: &'t mut WriteTxn<'db>,
    mode: Keys,
    deleted: Vec<u32>,
}

impl KeyChanges<'_, '_> {
    fn add<K: Codec>(&mut self, key: &K, ordinal: u32) -> io::Result<()> {
        let entry = key_entry(key);
        let mut live = self.txn.get(&entry).map(|b| ordinals(&b)).unwrap_or_default();
        if self.mode == Keys::Unique {
            self.deleted.append(&mut live);
        }
        live.push(ordinal);
        let bytes: Vec<u8> = live.iter().flat_map(|o| o.to_be_bytes()).collect();
        self.txn.put(&entry, &bytes)
    }

    fn delete<K: Codec>(&mut self, key: &K) {
        let entry = key_entry(key);
        if let Some(live) = self.txn.get(&entry) {
            self.deleted.extend(ordinals(&live));
            self.txn.delete(&entry);
        }
    }

    /// Marks the deleted ordinals and records what the table now accounts for; returns the ordinals.
    fn finish(self, sources: u32, ops: u64) -> io::Result<Vec<u32>> {
        for &o in &self.deleted {
            let mut entry = vec![DELETED];
            entry.extend_from_slice(&o.to_be_bytes());
            self.txn.put(&entry, &[])?;
        }
        let mut meta = (sources as u64).to_le_bytes().to_vec();
        meta.extend_from_slice(&ops.to_le_bytes());
        self.txn.put(META, &meta)?;
        Ok(self.deleted)
    }
}

struct Writer<R, K> {
    pending: Vec<Op<R, K>>,
    /// Operations logged since the collection began, up to the pending ones.
    ops: u64,
}

pub struct Collection<D: Domain> {
    #[cfg(test)]
    crash: std::sync::atomic::AtomicU8,
    domain: D,
    index: Index<D::Key, D::Unit>,
    keys: Db,
    log: Log,
    writer: Mutex<Writer<D::Record, D::Key>>,
    current: RwLock<Arc<Generation<D>>>,
}

impl<D: Domain> Collection<D> {
    /// Opens (or creates) a collection in `files`, recovering from its log.
    pub fn open(files: &Store, domain: D) -> io::Result<Collection<D>> {
        let index: Index<D::Key, D::Unit> = Index::open_in(&files.store("units")?, domain.index_options())?;
        let keys = Db::open(files.path("keys.db")?, b_positive::Options::default())?;
        let log = Log::open(files.path("log")?)?;
        let snapshot = index.snapshot();
        let sources = snapshot.source_count();
        let side = domain.open(&snapshot)?;
        let collection = Collection {
            #[cfg(test)]
            crash: std::sync::atomic::AtomicU8::new(0),
            domain,
            index,
            keys,
            log,
            writer: Mutex::new(Writer { pending: Vec::new(), ops: 0 }),
            current: RwLock::new(Arc::new(Generation { snapshot, side, deleted: Arc::new(SourceSet::new(sources)), sources })),
        };
        collection.recover()?;
        Ok(collection)
    }

    pub fn domain(&self) -> &D {
        &self.domain
    }

    /// The current generation (the last commit).
    pub fn current(&self) -> Arc<Generation<D>> {
        self.current.read().unwrap().clone()
    }

    /// Operations logged but not yet committed.
    pub fn pending(&self) -> usize {
        self.writer.lock().unwrap().pending.len()
    }

    fn deleted_set(&self, capacity: u32, more: impl IntoIterator<Item = u32>) -> SourceSet {
        let read = self.keys.begin_read();
        let mut set = SourceSet::new(capacity);
        for (key, _) in read.range_from(&[DELETED]) {
            if key[0] != DELETED {
                break;
            }
            set.insert(u32::from_be_bytes(key[1..5].try_into().unwrap()));
        }
        for o in more {
            set.insert(o);
        }
        set
    }

    fn recover(&self) -> io::Result<()> {
        let current = self.current();
        let sources = current.sources;
        let (accounted_sources, accounted_ops) = self.keys.begin_read().get(META).map_or((0, 0), |m| (u64_at(&m, 0) as u32, u64_at(&m, 8)));
        // the log's checkpoint; a log from before the collection layer holds a source count only, and adds only
        let checkpoint = self.log.checkpoint().unwrap_or_default();
        let (base_sources, base_ops, legacy) = match checkpoint.len() {
            CHECKPOINT => (u64_at(&checkpoint, 0) as u32, u64_at(&checkpoint, 8), false),
            4 => (u32::from_le_bytes(checkpoint[0..4].try_into().unwrap()), accounted_ops, true),
            8 => (u64_at(&checkpoint, 0) as u32, accounted_ops, true),
            _ => (sources, accounted_ops, false),
        };
        let mut ops = Vec::new();
        self.log.replay(|record| {
            let damaged = || invalid("a damaged log record");
            ops.push(match (legacy, record.first()) {
                (true, _) => Op::Add(self.domain.decode(record).ok_or_else(damaged)?),
                (false, Some(&ADD)) => Op::Add(self.domain.decode(&record[1..]).ok_or_else(damaged)?),
                (false, Some(&DELETE)) => Op::Delete(D::Key::decode(&record[1..])),
                _ => return Err(damaged()),
            });
            Ok(())
        })?;
        let logged = ops.len() as u64;

        let mut txn = self.keys.begin_write();
        let mut changes = KeyChanges { txn: &mut txn, mode: self.domain.keys(), deleted: Vec::new() };
        // sources before the log that the key table doesn't account for (an index from before it): their keys
        for o in accounted_sources..base_sources.min(sources) {
            changes.add(&current.snapshot.key(o), o)?;
        }
        // the index holds sources past the log's base: the log is that batch, committed to the index but not to
        // the key table, whose changes are made now, in order
        let committed_batch = sources > base_sources;
        let mut pending = Vec::new();
        let mut ordinal = base_sources;
        for (n, op) in ops.into_iter().enumerate() {
            let accounted = base_ops + (n as u64) < accounted_ops;
            match op {
                Op::Add(record) => {
                    if accounted {
                    } else if ordinal < sources {
                        changes.add(&self.domain.key(&record), ordinal)?;
                    } else {
                        pending.push(Op::Add(record));
                    }
                    ordinal += 1;
                }
                Op::Delete(key) => {
                    if accounted {
                    } else if committed_batch {
                        changes.delete(&key);
                    } else {
                        pending.push(Op::Delete(key));
                    }
                }
            }
        }
        let done = base_ops + logged - pending.len() as u64;
        let deleted = changes.finish(sources, done)?;
        txn.commit()?;

        let set = self.deleted_set(sources, deleted);
        *self.current.write().unwrap() = Arc::new(Generation { snapshot: self.index.snapshot(), side: current.side.clone(), deleted: Arc::new(set), sources });
        let mut w = self.writer.lock().unwrap();
        w.ops = done;
        w.pending = pending;
        if w.pending.is_empty() {
            return self.reset_log(&w);
        }
        drop(w);
        self.commit()
    }

    /// In tests, a commit can stop at a point as a crash would: 1 before the index commits, 2 after it (before the
    /// key table), 3 after the key table (before the log resets).
    #[cfg(test)]
    fn crash_at(&self, point: u8) -> io::Result<()> {
        if self.crash.load(std::sync::atomic::Ordering::Relaxed) == point {
            return Err(io::Error::other("crash"));
        }
        Ok(())
    }

    #[cfg(not(test))]
    #[inline]
    fn crash_at(&self, _: u8) -> io::Result<()> {
        Ok(())
    }

    fn reset_log(&self, w: &Writer<D::Record, D::Key>) -> io::Result<()> {
        let mut checkpoint = (self.current().sources as u64).to_le_bytes().to_vec();
        checkpoint.extend_from_slice(&w.ops.to_le_bytes());
        self.log.reset(&checkpoint)
    }

    fn log_ops(&self, records: Vec<Vec<u8>>) -> io::Result<()> {
        self.log.append_all(records.iter().map(Vec::as_slice))?;
        self.log.sync()
    }

    /// Adds records (with [`Keys::Unique`], replacing live ones with the same key); durable when this returns,
    /// searchable after the next commit.
    pub fn add(&self, records: Vec<D::Record>) -> io::Result<()> {
        for r in &records {
            self.domain.check(r).map_err(invalid)?;
        }
        let logged: Vec<Vec<u8>> = records
            .iter()
            .map(|r| {
                let mut out = vec![ADD];
                self.domain.encode(r, &mut out);
                out
            })
            .collect();
        let mut w = self.writer.lock().unwrap();
        self.log_ops(logged)?;
        w.pending.extend(records.into_iter().map(Op::Add));
        Ok(())
    }

    /// Deletes every live source with these keys (a key with none is ignored); durable when this returns, applied
    /// at the next commit.
    pub fn delete(&self, keys: Vec<D::Key>) -> io::Result<()> {
        let logged: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| {
                let mut out = vec![DELETE];
                k.encode(&mut out);
                out
            })
            .collect();
        let mut w = self.writer.lock().unwrap();
        self.log_ops(logged)?;
        w.pending.extend(keys.into_iter().map(Op::Delete));
        Ok(())
    }

    /// Applies everything logged since the last commit and publishes it.
    pub fn commit(&self) -> io::Result<()> {
        let mut w = self.writer.lock().unwrap();
        if w.pending.is_empty() {
            return Ok(());
        }
        let ops = std::mem::take(&mut w.pending);
        let count = ops.len() as u64;
        let mut batch = Vec::new();
        let mut order: Vec<Op<(), D::Key>> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Op::Add(r) => {
                    batch.push(r);
                    order.push(Op::Add(()));
                }
                Op::Delete(k) => order.push(Op::Delete(k)),
            }
        }
        let current = self.current();
        let first = current.sources;
        let side = if batch.is_empty() {
            current.side.clone()
        } else {
            let side = self.domain.apply(&batch, first, &current.side, &self.index)?;
            self.crash_at(1)?;
            self.index.commit()?;
            side
        };
        let sources = self.index.snapshot().source_count();
        if sources != first + batch.len() as u32 {
            return Err(invalid(format!("the domain added {} sources for {} records", sources - first, batch.len())));
        }

        let mut txn = self.keys.begin_write();
        let mut changes = KeyChanges { txn: &mut txn, mode: self.domain.keys(), deleted: Vec::new() };
        let mut added = batch.iter().zip(first..);
        for op in &order {
            match op {
                Op::Add(()) => {
                    let (record, ordinal) = added.next().unwrap();
                    changes.add(&self.domain.key(record), ordinal)?;
                }
                Op::Delete(key) => changes.delete(key),
            }
        }
        self.crash_at(2)?;
        let deleted = changes.finish(sources, w.ops + count)?;
        txn.commit()?;
        w.ops += count;
        self.crash_at(3)?;

        let mut set = SourceSet::new(sources);
        for o in current.deleted.iter().chain(deleted) {
            set.insert(o);
        }
        *self.current.write().unwrap() = Arc::new(Generation { snapshot: self.index.snapshot(), side, deleted: Arc::new(set), sources });
        self.reset_log(&w)
    }

    /// Fills an empty collection in one commit without the log, as `apply` adds the sources (in any order, such as
    /// by cell; it returns the side data). Their keys are then read from the index. For bulk loads.
    pub fn load(&self, apply: impl FnOnce(&Index<D::Key, D::Unit>) -> io::Result<D::Side>) -> io::Result<()> {
        let w = self.writer.lock().unwrap();
        if self.current().sources > 0 || !w.pending.is_empty() {
            return Err(invalid("load needs an empty collection"));
        }
        let side = apply(&self.index)?;
        self.index.commit()?;
        let snapshot = self.index.snapshot();
        let sources = snapshot.source_count();
        let mut txn = self.keys.begin_write();
        let mut changes = KeyChanges { txn: &mut txn, mode: self.domain.keys(), deleted: Vec::new() };
        for o in 0..sources {
            changes.add(&snapshot.key(o), o)?;
        }
        let deleted = changes.finish(sources, w.ops)?;
        txn.commit()?;
        let mut set = SourceSet::new(sources);
        for o in deleted {
            set.insert(o);
        }
        *self.current.write().unwrap() = Arc::new(Generation { snapshot, side, deleted: Arc::new(set), sources });
        self.reset_log(&w)
    }

    /// Commits what's pending, for a clean shutdown.
    pub fn close(&self) -> io::Result<()> {
        self.commit()
    }
}

/// Helpers for encoding log records.
pub mod codec {
    pub use introvert::varint::{read as read_varint, write as write_varint};

    pub fn write_string(out: &mut Vec<u8>, s: &str) {
        write_varint(out, s.len() as u64);
        out.extend_from_slice(s.as_bytes());
    }

    /// A string written by [`write_string`], or None if the bytes end first.
    pub fn read_string(bytes: &[u8], at: &mut usize) -> Option<String> {
        let length = read_varint(bytes, at) as usize;
        let s = bytes.get(*at..at.checked_add(length)?)?;
        *at += length;
        Some(String::from_utf8_lossy(s).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// A record: a key, words (its units) and a value kept as side data, a column of u64 by source.
    #[derive(Clone, Debug)]
    struct Note {
        key: String,
        words: Vec<String>,
        value: u64,
    }

    struct Notes {
        values: larry::Blob,
        keys: Keys,
    }

    impl Domain for Notes {
        type Key = String;
        type Unit = String;
        type Record = Note;
        type Side = Arc<Vec<u64>>;

        fn index_options(&self) -> Options {
            Options { frequencies: false, positions: false, ..Options::default() }
        }

        fn keys(&self) -> Keys {
            self.keys
        }

        fn encode(&self, r: &Note, out: &mut Vec<u8>) {
            codec::write_string(out, &r.key);
            codec::write_varint(out, r.value);
            codec::write_varint(out, r.words.len() as u64);
            for w in &r.words {
                codec::write_string(out, w);
            }
        }

        fn decode(&self, bytes: &[u8]) -> Option<Note> {
            let at = &mut 0;
            let key = codec::read_string(bytes, at)?;
            let value = codec::read_varint(bytes, at);
            let n = codec::read_varint(bytes, at);
            let words = (0..n).map(|_| codec::read_string(bytes, at)).collect::<Option<Vec<_>>>()?;
            Some(Note { key, words, value })
        }

        fn key(&self, r: &Note) -> String {
            r.key.clone()
        }

        fn check(&self, r: &Note) -> Result<(), String> {
            if r.key.is_empty() { Err("an empty key".into()) } else { Ok(()) }
        }

        fn open(&self, snapshot: &Snapshot<String, String>) -> io::Result<Arc<Vec<u64>>> {
            let count = snapshot.source_count() as u64;
            self.values.truncate(count * 8)?;
            let bytes = self.values.read_all()?;
            Ok(Arc::new(bytes.as_chunks::<8>().0.iter().map(|c| u64::from_le_bytes(*c)).collect()))
        }

        fn apply(&self, batch: &[Note], first: u32, side: &Arc<Vec<u64>>, index: &Index<String, String>) -> io::Result<Arc<Vec<u64>>> {
            assert_eq!(side.len(), first as usize);
            let bytes: Vec<u8> = batch.iter().flat_map(|r| r.value.to_le_bytes()).collect();
            self.values.append(&bytes)?;
            for r in batch {
                index.add(&r.key, r.words.iter().map(|w| (w.clone(), 0)));
            }
            let mut values = (**side).clone();
            values.extend(batch.iter().map(|r| r.value));
            Ok(Arc::new(values))
        }
    }

    fn open(dir: &std::path::Path, keys: Keys) -> Collection<Notes> {
        let files = Store::open(dir).unwrap();
        let domain = Notes { values: files.blob("values").unwrap(), keys };
        Collection::open(&files, domain).unwrap()
    }

    fn note(key: &str, words: &str, value: u64) -> Note {
        Note { key: key.into(), words: words.split(' ').map(str::to_string).collect(), value }
    }

    /// Every live source: key -> its values (sorted).
    fn live(c: &Collection<Notes>) -> BTreeMap<String, Vec<u64>> {
        let g = c.current();
        let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for s in 0..g.sources {
            if g.is_live(s) {
                out.entry(g.snapshot.key(s)).or_default().push(g.side[s as usize]);
            }
        }
        out.values_mut().for_each(|v| v.sort());
        out
    }

    /// Live sources with a word.
    fn with_word(c: &Collection<Notes>, word: &str) -> BTreeSet<String> {
        let g = c.current();
        let mut out = BTreeSet::new();
        if let Some(p) = g.snapshot.unit(&word.to_string()) {
            let mut cursor = g.snapshot.postings.cursor(&p);
            while cursor.next() {
                if g.is_live(cursor.source) {
                    out.insert(g.snapshot.key(cursor.source));
                }
            }
        }
        out
    }

    #[test]
    fn adds_replaces_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let c = open(dir.path(), Keys::Unique);
        c.add(vec![note("a", "red apple", 1), note("b", "green apple", 2), note("c", "red cherry", 3)]).unwrap();
        assert_eq!(c.current().sources, 0);
        c.commit().unwrap();
        assert_eq!(with_word(&c, "red"), ["a", "c"].map(String::from).into());
        c.add(vec![note("a", "green pear", 10)]).unwrap();
        c.delete(vec!["c".into(), "nope".into()]).unwrap();
        c.commit().unwrap();
        assert_eq!(live(&c), [("a".into(), vec![10]), ("b".into(), vec![2])].into());
        assert_eq!(with_word(&c, "red"), BTreeSet::new());
        assert_eq!(with_word(&c, "green"), ["a", "b"].map(String::from).into());
        assert_eq!((c.current().sources, c.current().live()), (4, 2));
        assert!(c.add(vec![note("", "x", 0)]).is_err());
        drop(c);
        let c = open(dir.path(), Keys::Unique);
        assert_eq!(live(&c), [("a".into(), vec![10]), ("b".into(), vec![2])].into());

        let dir = tempfile::tempdir().unwrap();
        let c = open(dir.path(), Keys::Multiple);
        c.add(vec![note("a", "x", 1), note("a", "y", 2), note("b", "x", 3)]).unwrap();
        c.commit().unwrap();
        assert_eq!(live(&c)["a"], [1, 2]);
        c.delete(vec!["a".into()]).unwrap();
        c.commit().unwrap();
        assert_eq!(live(&c), [("b".into(), vec![3])].into());
    }

    #[test]
    fn delete_then_add_in_one_batch_keeps_the_add() {
        let dir = tempfile::tempdir().unwrap();
        let c = open(dir.path(), Keys::Unique);
        c.add(vec![note("a", "x", 1)]).unwrap();
        c.commit().unwrap();
        c.delete(vec!["a".into()]).unwrap();
        c.add(vec![note("a", "y", 2)]).unwrap();
        c.add(vec![note("b", "y", 3)]).unwrap();
        c.delete(vec!["b".into()]).unwrap();
        c.commit().unwrap();
        assert_eq!(live(&c), [("a".into(), vec![2])].into());
    }

    #[test]
    fn a_crash_at_each_point_of_a_commit_keeps_the_batch() {
        for point in 1..=3 {
            for keys in [Keys::Unique, Keys::Multiple] {
                let dir = tempfile::tempdir().unwrap();
                let c = open(dir.path(), keys);
                c.add(vec![note("a", "x", 1), note("b", "x", 2)]).unwrap();
                c.commit().unwrap();
                c.add(vec![note("a", "y", 3)]).unwrap();
                c.delete(vec!["b".into()]).unwrap();
                c.add(vec![note("c", "y", 4)]).unwrap();
                c.crash.store(point, std::sync::atomic::Ordering::Relaxed);
                assert!(c.commit().is_err());
                drop(c);
                let c = open(dir.path(), keys);
                let a = if keys == Keys::Unique { vec![3] } else { vec![1, 3] };
                assert_eq!(live(&c), [("a".into(), a), ("c".into(), vec![4])].into(), "crash at {point}, {keys:?}");
                assert_eq!(c.current().sources, 4);
            }
        }
    }

    /// Random adds, replacements, deletes, commits and crashes (at each point of a commit, and between), against a
    /// model; after every reopen the collection holds exactly what was acknowledged.
    #[test]
    fn survives_crashes_at_every_point() {
        for (seed, keys) in [(1u64, Keys::Unique), (2, Keys::Multiple), (3, Keys::Unique), (4, Keys::Multiple)] {
            let mut state = seed;
            let mut random = move |n: u64| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (state >> 33) % n
            };
            let dir = tempfile::tempdir().unwrap();
            let mut c = open(dir.path(), keys);
            let mut model: BTreeMap<String, Vec<u64>> = BTreeMap::new();
            let mut next = 0u64;
            for step in 0..400 {
                match random(10) {
                    0..=4 => {
                        let batch: Vec<Note> = (0..1 + random(4))
                            .map(|_| {
                                next += 1;
                                note(&format!("k{}", random(12)), &format!("w{} w{}", random(5), random(5)), next)
                            })
                            .collect();
                        for n in &batch {
                            let values = model.entry(n.key.clone()).or_default();
                            if keys == Keys::Unique {
                                values.clear();
                            }
                            values.push(n.value);
                        }
                        c.add(batch).unwrap();
                    }
                    5 => {
                        let key = format!("k{}", random(12));
                        model.remove(&key);
                        c.delete(vec![key]).unwrap();
                    }
                    6 | 7 => c.commit().unwrap(),
                    8 => {
                        c.crash.store(1 + random(3) as u8, std::sync::atomic::Ordering::Relaxed);
                        let _ = c.commit();
                        drop(c);
                        c = open(dir.path(), keys);
                    }
                    _ => {
                        drop(c);
                        c = open(dir.path(), keys);
                    }
                }
                model.retain(|_, v| !v.is_empty());
                model.values_mut().for_each(|v| v.sort());
                if c.pending() == 0 {
                    assert_eq!(live(&c), model, "seed {seed}, step {step}");
                }
            }
            c.commit().unwrap();
            assert_eq!(live(&c), model, "seed {seed}, at the end");
        }
    }
}
