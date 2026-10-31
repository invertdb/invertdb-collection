//! The key table: which sources each key names, and which sources are deleted. Keys themselves aren't stored again
//! (the index's source table has each source's key); the table finds a key's sources by a hash of its encoding and
//! checks each candidate's key in the index.
//!
//! - **The base** (blob `keys.{generation}`): the live sources [0, `base_upto`) as (the hash's high 32 bits, ordinal)
//!   pairs, sorted: 8 bytes a source. Hashes are uniform, so interpolation search finds a key's run in a few probes
//!   without reading any key; a key is read only for a source whose hash prefix matches.
//! - **The overlay** (in memory): sources from `base_upto` on, by full hash. It isn't stored: those sources are in the
//!   index, and opening hashes their keys again. Once it outgrows a quarter of the base (and [`FOLD_MIN`]), a commit
//!   merges it into a new base, dropping deleted sources, without reading any key.
//! - **Deleted sources** and **what the table accounts for** (the sources and log operations it has applied, and the
//!   base's generation, size and reach) are in `keys.db` (b-positive), written in one transaction per commit.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Mutex;

use b_positive::Db;
use introvert::{Codec, Snapshot, SourceSet};
use larry::{Store, View};

use crate::Keys;

const DELETED: u8 = b'd';
const META: &[u8] = b"m";

/// The overlay is merged into a new base once it holds more than this and a quarter of the base.
#[cfg(not(test))]
pub const FOLD_MIN: usize = 1 << 16;
#[cfg(test)]
pub const FOLD_MIN: usize = 8; // so tests fold often

/// A key's hash (FNV-1a over its encoding, then mixed).
pub fn hash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h = (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^ (h >> 33)
}

fn prefix(h: u64) -> u32 {
    (h >> 32) as u32
}

fn encoded<K: Codec>(key: &K) -> Vec<u8> {
    let mut out = Vec::new();
    key.encode(&mut out);
    out
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    pub sources: u32,
    pub ops: u64,
    pub generation: u64,
    /// Entries in the base.
    pub base_count: u32,
    /// The base holds the live sources before this.
    pub base_upto: u32,
}

impl Meta {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(40);
        for v in [self.sources as u64, self.ops, self.generation, self.base_count as u64, self.base_upto as u64] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Meta {
        let v = |i: usize| bytes.get(i * 8..i * 8 + 8).map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
        Meta { sources: v(0) as u32, ops: v(1), generation: v(2), base_count: v(3) as u32, base_upto: v(4) as u32 }
    }
}

fn base_name(generation: u64) -> String {
    format!("keys.{generation}")
}

/// The base as of the table's last commit.
struct Base {
    view: Option<View>,
    count: u32,
}

impl Base {
    #[inline]
    fn entry(&self, i: u32) -> (u32, u32) {
        let at = i as usize * 8;
        let bytes = &self.view.as_ref().unwrap()[at..at + 8];
        (u32::from_le_bytes(bytes[0..4].try_into().unwrap()), u32::from_le_bytes(bytes[4..8].try_into().unwrap()))
    }

    /// The ordinals whose hash prefix is `p` (interpolation search: prefixes are uniform).
    fn run(&self, p: u32) -> Vec<u32> {
        let (mut low, mut high) = (0u32, self.count); // the first entry with prefix >= p is in [low, high]
        while low < high {
            let (first, last) = (self.entry(low).0, self.entry(high - 1).0);
            if p <= first {
                break;
            }
            if p > last {
                low = high;
                break;
            }
            // a guess between low and high-1 by where p falls between their prefixes, then shrink around it
            let span = (last - first) as u64;
            let guess = low + (((p - first) as u64 * (high - 1 - low) as u64) / span.max(1)) as u32;
            let guess = guess.clamp(low, high - 1);
            if self.entry(guess).0 < p {
                low = guess + 1;
            } else {
                high = guess;
            }
        }
        let mut out = Vec::new();
        let mut i = low;
        while i < self.count {
            let (q, o) = self.entry(i);
            if q != p {
                break;
            }
            out.push(o);
            i += 1;
        }
        out
    }
}

pub struct KeyTable {
    files: Store,
    db: Db,
    mode: Keys,
    /// Sources from the base's reach on, by hash.
    overlay: Mutex<HashMap<u64, Vec<u32>>>,
}

impl KeyTable {
    pub fn open(files: &Store, mode: Keys) -> io::Result<KeyTable> {
        let db = Db::open(files.path("keys.db")?, b_positive::Options::default())?;
        let table = KeyTable { files: files.clone(), db, mode, overlay: Mutex::new(HashMap::new()) };
        // a crash between writing a new base and recording it, or between recording it and removing the old one,
        // leaves a base file no generation names
        let meta = table.meta();
        for stale in [meta.generation.wrapping_sub(1), meta.generation + 1] {
            if stale != meta.generation {
                table.files.remove(&base_name(stale))?;
            }
        }
        Ok(table)
    }

    pub fn meta(&self) -> Meta {
        self.db.begin_read().get(META).map_or_else(Meta::default, |m| Meta::decode(&m))
    }

    fn base(&self, meta: &Meta) -> io::Result<Base> {
        if meta.base_count == 0 {
            return Ok(Base { view: None, count: 0 });
        }
        Ok(Base { view: Some(self.files.blob(&base_name(meta.generation))?.view()?), count: meta.base_count })
    }

    /// Every deleted source, in a set of `capacity`.
    pub fn deleted(&self, capacity: u32) -> SourceSet {
        let read = self.db.begin_read();
        let mut set = SourceSet::new(capacity);
        for (key, _) in read.range_from(&[DELETED]) {
            if key[0] != DELETED {
                break;
            }
            set.insert(u32::from_be_bytes(key[1..5].try_into().unwrap()));
        }
        set
    }

    /// Hashes the keys of the sources the table accounts for past its base, into the overlay (on opening).
    pub fn restore<K: Codec, U: Codec>(&self, snapshot: &Snapshot<K, U>, deleted: &SourceSet) {
        let meta = self.meta();
        let mut overlay = self.overlay.lock().unwrap();
        overlay.clear();
        if self.mode == Keys::Ordinal {
            return;
        }
        for o in meta.base_upto..meta.sources.min(snapshot.source_count()) {
            if !(o < deleted.capacity() && deleted.contains(o)) {
                overlay.entry(hash(&encoded(&snapshot.key(o)))).or_default().push(o);
            }
        }
    }

    /// Starts a batch of changes, against `snapshot` (which holds every source the batch names) and the sources
    /// deleted so far.
    pub fn batch<'t, K: Codec, U: Codec>(&'t self, snapshot: &'t Snapshot<K, U>, deleted: &'t SourceSet) -> io::Result<Batch<'t, K, U>> {
        let meta = self.meta();
        let base = self.base(&meta)?;
        Ok(Batch { table: self, snapshot, deleted, base, meta, local: HashMap::new(), hashed: Vec::new(), newly_deleted: HashSet::new() })
    }

    /// Writes a base of `entries` ((hash prefix, ordinal), sorted) as the next generation and records it, with the
    /// sources and operations accounted for and the newly deleted sources, in one transaction; empties the overlay.
    fn replace_base(&self, entries: &[(u32, u32)], upto: u32, sources: u32, ops: u64, deleted: &[u32]) -> io::Result<()> {
        let meta = self.meta();
        let generation = meta.generation + 1;
        let blob = self.files.blob(&base_name(generation))?;
        blob.truncate(0)?;
        let bytes: Vec<u8> = entries.iter().flat_map(|&(p, o)| p.to_le_bytes().into_iter().chain(o.to_le_bytes())).collect();
        blob.append(&bytes)?;
        blob.sync()?;
        let mut txn = self.db.begin_write();
        for &o in deleted {
            let mut entry = vec![DELETED];
            entry.extend_from_slice(&o.to_be_bytes());
            txn.put(&entry, &[])?;
        }
        txn.put(META, &Meta { sources, ops, generation, base_count: entries.len() as u32, base_upto: upto }.encode())?;
        txn.commit()?;
        self.overlay.lock().unwrap().clear();
        if meta.base_count > 0 || meta.generation > 0 {
            self.files.remove(&base_name(meta.generation))?;
        }
        Ok(())
    }

    /// Builds the table for sources [0, `sources`) from the index's keys, as one base (with [`Keys::Unique`], a key's
    /// older sources are marked deleted): for bulk loads and for collections from before the key table. `ops` is what
    /// the table then accounts for of the log. Returns the newly deleted sources.
    pub fn build<K: Codec, U: Codec>(&self, snapshot: &Snapshot<K, U>, sources: u32, ops: u64) -> io::Result<Vec<u32>> {
        if self.mode == Keys::Ordinal {
            self.replace_base(&[], sources, sources, ops, &[])?;
            return Ok(Vec::new());
        }
        let already = self.deleted(sources);
        let mut order: Vec<(u64, u32)> = (0..sources).filter(|&o| !already.contains(o)).map(|o| (hash(&encoded(&snapshot.key(o))), o)).collect();
        order.sort_unstable();
        let mut deleted = Vec::new();
        if self.mode == Keys::Unique {
            // within each run of one hash, every source but a key's last is replaced
            let mut i = 0;
            while i < order.len() {
                let mut j = i + 1;
                while j < order.len() && order[j].0 == order[i].0 {
                    j += 1;
                }
                if j - i > 1 {
                    let mut by_key: HashMap<Vec<u8>, u32> = HashMap::new();
                    for &(_, o) in &order[i..j] {
                        if let Some(previous) = by_key.insert(encoded(&snapshot.key(o)), o) {
                            deleted.push(previous); // ordinals ascend within a run
                        }
                    }
                }
                i = j;
            }
        }
        let gone: HashSet<u32> = deleted.iter().copied().collect();
        let mut entries: Vec<(u32, u32)> = order.iter().filter(|(_, o)| !gone.contains(o)).map(|&(h, o)| (prefix(h), o)).collect();
        entries.sort_unstable();
        self.replace_base(&entries, sources, sources, ops, &deleted)?;
        Ok(deleted)
    }
}

/// One commit's changes to the key table.
pub struct Batch<'t, K, U> {
    table: &'t KeyTable,
    snapshot: &'t Snapshot<K, U>,
    deleted: &'t SourceSet,
    base: Base,
    meta: Meta,
    /// Sources this batch added, by key encoding.
    local: HashMap<Vec<u8>, Vec<u32>>,
    hashed: Vec<(u64, u32)>,
    newly_deleted: HashSet<u32>,
}

impl<K: Codec, U: Codec> Batch<'_, K, U> {
    fn is_deleted(&self, ordinal: u32) -> bool {
        (ordinal < self.deleted.capacity() && self.deleted.contains(ordinal)) || self.newly_deleted.contains(&ordinal)
    }

    /// The live sources a key (encoded, and its hash) names.
    fn live(&self, key: &[u8], h: u64) -> Vec<u32> {
        let mut found = self.base.run(prefix(h));
        if let Some(more) = self.table.overlay.lock().unwrap().get(&h) {
            found.extend_from_slice(more);
        }
        // a hash (or its prefix) names the key's sources and maybe others': check each one's key
        found.retain(|&o| !self.is_deleted(o) && encoded(&self.snapshot.key(o)) == key);
        found.extend(self.local.get(key).into_iter().flatten().copied().filter(|&o| !self.is_deleted(o)));
        found
    }

    pub fn add(&mut self, key: &K, ordinal: u32) -> io::Result<()> {
        if self.table.mode == Keys::Ordinal {
            return Ok(());
        }
        let key = encoded(key);
        let h = hash(&key);
        if self.table.mode == Keys::Unique {
            for o in self.live(&key, h) {
                self.newly_deleted.insert(o);
            }
        }
        self.local.entry(key).or_default().push(ordinal);
        self.hashed.push((h, ordinal));
        Ok(())
    }

    pub fn delete(&mut self, key: &K) {
        let key = encoded(key);
        let h = hash(&key);
        for o in self.live(&key, h) {
            self.newly_deleted.insert(o);
        }
    }

    /// Deletes a source by its ordinal ([`Keys::Ordinal`]); one the index doesn't hold, or already deleted, is ignored.
    pub fn delete_ordinal(&mut self, ordinal: u32) {
        if ordinal < self.snapshot.source_count() && !self.is_deleted(ordinal) {
            self.newly_deleted.insert(ordinal);
        }
    }

    /// Records the changes, and that the table accounts for `sources` and `ops`; returns the newly deleted sources.
    /// Merges the overlay into a new base once it has outgrown a quarter of the old one.
    pub fn finish(self, sources: u32, ops: u64) -> io::Result<Vec<u32>> {
        let mut deleted: Vec<u32> = self.newly_deleted.iter().copied().collect();
        deleted.sort_unstable();
        let overlay_size = self.table.overlay.lock().unwrap().values().map(Vec::len).sum::<usize>() + self.hashed.len();
        if overlay_size > FOLD_MIN.max(self.meta.base_count as usize / 4) {
            // a new base: the old one and the overlay, merged, without deleted sources
            let gone = |o: u32| self.is_deleted(o);
            let mut extra: Vec<(u32, u32)> = self.table.overlay.lock().unwrap().iter().flat_map(|(&h, os)| os.iter().map(move |&o| (prefix(h), o))).collect();
            extra.extend(self.hashed.iter().map(|&(h, o)| (prefix(h), o)));
            extra.retain(|&(_, o)| !gone(o));
            extra.sort_unstable();
            let mut entries = Vec::with_capacity(self.base.count as usize + extra.len());
            let mut extra = extra.into_iter().peekable();
            for i in 0..self.base.count {
                let e = self.base.entry(i);
                while extra.peek().is_some_and(|&x| x < e) {
                    entries.push(extra.next().unwrap());
                }
                if !gone(e.1) {
                    entries.push(e);
                }
            }
            entries.extend(extra);
            self.table.replace_base(&entries, sources, sources, ops, &deleted)?;
            return Ok(deleted);
        }
        let mut txn = self.table.db.begin_write();
        for &o in &deleted {
            let mut entry = vec![DELETED];
            entry.extend_from_slice(&o.to_be_bytes());
            txn.put(&entry, &[])?;
        }
        txn.put(META, &Meta { sources, ops, ..self.meta }.encode())?;
        txn.commit()?;
        let mut overlay = self.table.overlay.lock().unwrap();
        for (h, o) in self.hashed {
            overlay.entry(h).or_default().push(o);
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Interpolation search finds every prefix's run, as a scan does, over uniform and clumped prefixes.
    #[test]
    fn runs_are_found_as_a_scan_finds_them() {
        let dir = tempfile::tempdir().unwrap();
        let files = Store::open(dir.path()).unwrap();
        let mut seed = 3u64;
        let mut random = move || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            seed >> 32
        };
        for (n, clumped) in [(0u32, false), (1, false), (7, false), (5000, false), (5000, true)] {
            let mut entries: Vec<(u32, u32)> = (0..n).map(|o| (if clumped { (random() % 40) as u32 * 1000 } else { random() as u32 }, o)).collect();
            let long = entries.first().map_or(5, |e| e.0);
            entries.extend((0..20).map(|o| (long, n + o))); // a long run
            entries.sort_unstable();
            let blob = files.blob("b").unwrap();
            blob.truncate(0).unwrap();
            blob.append(&entries.iter().flat_map(|&(p, o)| p.to_le_bytes().into_iter().chain(o.to_le_bytes())).collect::<Vec<u8>>()).unwrap();
            let base = Base { view: Some(blob.view().unwrap()), count: entries.len() as u32 };
            let mut probes: Vec<u32> = entries.iter().map(|e| e.0).collect();
            probes.extend([0, 1, u32::MAX, u32::MAX - 1]);
            probes.extend((0..200).map(|_| random() as u32));
            for p in probes {
                let expected: Vec<u32> = entries.iter().filter(|e| e.0 == p).map(|e| e.1).collect();
                assert_eq!(base.run(p), expected, "prefix {p}, {n} entries");
            }
        }
    }
}
