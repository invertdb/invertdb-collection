//! The key table: which sources each key names, and which sources are deleted. Keys themselves aren't stored again
//! (the index's source table has each source's key); the table finds a key's sources by a hash of its encoding and
//! checks each candidate's key in the index.
//!
//! - **The base** (blob `keys.{generation}`): the sources [0, `base_count`) that aren't deleted, as u32 ordinals in
//!   order of (hash of the key, ordinal): 4 bytes a source. Binary search finds a key's run. Bulk loads, collections
//!   from before the key table, and rebuilds write it.
//! - **The overlay** (`keys.db`, b-positive): `h` + hash (u64 big-endian) + ordinal (u32 big-endian) for sources
//!   added since the base. When it outgrows the base (and [`REBUILD_MIN`]), a commit writes a new base over every
//!   live source and empties the overlay.
//! - **Deleted sources:** `d` + ordinal in `keys.db`.
//! - **What the table accounts for** (`m`): the sources and log operations it has applied, the base's generation and
//!   count, and the overlay's count; written in the same transaction as the changes, so a commit's key changes are
//!   one atomic step.

use std::collections::{HashMap, HashSet};
use std::io;

use b_positive::{Db, ReadTxn, WriteTxn};
use introvert::{Codec, Snapshot, SourceSet};
use larry::{Blob, Store, View};

use crate::Keys;

const HASHED: u8 = b'h';
const DELETED: u8 = b'd';
const META: &[u8] = b"m";

/// The overlay is folded into a new base once it holds more entries than this and than the base.
#[cfg(not(test))]
pub const REBUILD_MIN: u64 = 1 << 16;
#[cfg(test)]
pub const REBUILD_MIN: u64 = 8; // so tests fold often

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
    pub base_count: u32,
    pub overlay: u64,
}

impl Meta {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(36);
        for v in [self.sources as u64, self.ops, self.generation, self.base_count as u64, self.overlay] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Meta {
        let v = |i: usize| u64::from_le_bytes(bytes[i * 8..i * 8 + 8].try_into().unwrap());
        Meta { sources: v(0) as u32, ops: v(1), generation: v(2), base_count: v(3) as u32, overlay: v(4) }
    }
}

pub struct KeyTable {
    files: Store,
    db: Db,
    mode: Keys,
}

/// The base as of the table's last commit.
struct Base {
    view: Option<View>,
    count: u32,
}

impl Base {
    fn ordinal(&self, i: u32) -> u32 {
        let at = i as usize * 4;
        u32::from_le_bytes(self.view.as_ref().unwrap()[at..at + 4].try_into().unwrap())
    }
}

fn base_name(generation: u64) -> String {
    format!("keys.{generation}")
}

impl KeyTable {
    pub fn open(files: &Store, mode: Keys) -> io::Result<KeyTable> {
        let db = Db::open(files.path("keys.db")?, b_positive::Options::default())?;
        Ok(KeyTable { files: files.clone(), db, mode })
    }

    pub fn meta(&self) -> Meta {
        self.db.begin_read().get(META).map_or_else(Meta::default, |m| Meta::decode(&m))
    }

    fn base(&self, meta: &Meta) -> io::Result<Base> {
        if meta.base_count == 0 {
            return Ok(Base { view: None, count: 0 });
        }
        let blob = self.files.blob(&base_name(meta.generation))?;
        Ok(Base { view: Some(blob.view()?), count: meta.base_count })
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

    /// Starts a batch of changes, against `snapshot` (which holds every source the batch names) and the sources
    /// deleted so far.
    pub fn batch<'t, K: Codec, U: Codec>(&'t self, snapshot: &'t Snapshot<K, U>, deleted: &'t SourceSet) -> io::Result<Batch<'t, K, U>> {
        let meta = self.meta();
        let base = self.base(&meta)?;
        Ok(Batch {
            table: self,
            snapshot,
            deleted,
            read: self.db.begin_read(),
            txn: self.db.begin_write(),
            base,
            meta,
            local: HashMap::new(),
            hashed: Vec::new(),
            newly_deleted: HashSet::new(),
        })
    }
}

/// One transaction's changes to the key table.
pub struct Batch<'t, K, U> {
    table: &'t KeyTable,
    snapshot: &'t Snapshot<K, U>,
    deleted: &'t SourceSet,
    read: ReadTxn,
    txn: WriteTxn<'t>,
    base: Base,
    meta: Meta,
    /// Sources this batch added, by key encoding.
    local: HashMap<Vec<u8>, Vec<u32>>,
    hashed: Vec<(u64, u32)>,
    newly_deleted: HashSet<u32>,
}

impl<K: Codec, U: Codec> Batch<'_, K, U> {
    fn hash_of(&self, ordinal: u32) -> u64 {
        hash(&encoded(&self.snapshot.key(ordinal)))
    }

    fn is_deleted(&self, ordinal: u32) -> bool {
        (ordinal < self.deleted.capacity() && self.deleted.contains(ordinal)) || self.newly_deleted.contains(&ordinal)
    }

    /// The live sources a key (encoded, and its hash) names.
    fn live(&self, key: &[u8], h: u64) -> Vec<u32> {
        let mut found = Vec::new();
        // the base: its run of this hash
        let (mut low, mut high) = (0, self.base.count);
        while low < high {
            let mid = (low + high) / 2;
            let o = self.base.ordinal(mid);
            if (self.hash_of(o), o) < (h, 0) { low = mid + 1 } else { high = mid }
        }
        while low < self.base.count {
            let o = self.base.ordinal(low);
            if self.hash_of(o) != h {
                break;
            }
            found.push(o);
            low += 1;
        }
        // the overlay, as committed before this batch
        let mut prefix = vec![HASHED];
        prefix.extend_from_slice(&h.to_be_bytes());
        for (entry, _) in self.read.range_from(&prefix) {
            if !entry.starts_with(&prefix) {
                break;
            }
            found.push(u32::from_be_bytes(entry[9..13].try_into().unwrap()));
        }
        // a hash names the key's sources and maybe others': check each one's key
        found.retain(|&o| !self.is_deleted(o) && encoded(&self.snapshot.key(o)) == key);
        found.extend(self.local.get(key).into_iter().flatten().copied().filter(|&o| !self.is_deleted(o)));
        found
    }

    pub fn add(&mut self, key: &K, ordinal: u32) -> io::Result<()> {
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

    /// Records the changes, and that the table accounts for `sources` and `ops`; returns the newly deleted sources.
    /// Folds the overlay into a new base if it has outgrown it.
    pub fn finish(mut self, sources: u32, ops: u64) -> io::Result<Vec<u32>> {
        let mut deleted: Vec<u32> = self.newly_deleted.iter().copied().collect();
        deleted.sort_unstable();
        for &o in &deleted {
            let mut entry = vec![DELETED];
            entry.extend_from_slice(&o.to_be_bytes());
            self.txn.put(&entry, &[])?;
        }
        let mut meta = Meta { sources, ops, ..self.meta };
        meta.overlay += self.hashed.len() as u64;
        let old_base = (meta.base_count > 0).then_some(meta.generation);
        if meta.overlay > REBUILD_MIN.max(meta.base_count as u64) {
            // a new base over every live source; the overlay is emptied
            let all: SourceSet = {
                let mut all = SourceSet::new(sources);
                for o in 0..sources {
                    if !self.is_deleted(o) {
                        all.insert(o);
                    }
                }
                all
            };
            meta.generation += 1;
            meta.base_count = write_base(&self.table.files, meta.generation, self.snapshot, &all)?;
            let overlay: Vec<Vec<u8>> = self.read.range_from(&[HASHED]).take_while(|(k, _)| k[0] == HASHED).map(|(k, _)| k.to_vec()).collect();
            for entry in overlay {
                self.txn.delete(&entry);
            }
            meta.overlay = 0;
        } else {
            let mut entries: Vec<Vec<u8>> = self
                .hashed
                .iter()
                .map(|&(h, o)| {
                    let mut entry = vec![HASHED];
                    entry.extend_from_slice(&h.to_be_bytes());
                    entry.extend_from_slice(&o.to_be_bytes());
                    entry
                })
                .collect();
            entries.sort_unstable();
            self.txn.put_sorted(&entries, |_, _| Vec::new())?;
        }
        self.txn.put(META, &meta.encode())?;
        self.txn.commit()?;
        if let Some(old) = old_base.filter(|&g| g != meta.generation) {
            self.table.files.remove(&base_name(old))?;
        }
        Ok(deleted)
    }
}

/// Writes a base of `live`'s sources (sorted by key hash) as generation `generation`; returns how many it holds.
fn write_base<K: Codec, U: Codec>(files: &Store, generation: u64, snapshot: &Snapshot<K, U>, live: &SourceSet) -> io::Result<u32> {
    let mut order: Vec<(u64, u32)> = live.iter().map(|o| (hash(&encoded(&snapshot.key(o))), o)).collect();
    order.sort_unstable();
    let blob: Blob = files.blob(&base_name(generation))?;
    blob.truncate(0)?;
    let bytes: Vec<u8> = order.iter().flat_map(|&(_, o)| o.to_le_bytes()).collect();
    blob.append(&bytes)?;
    blob.sync()?;
    Ok(order.len() as u32)
}

impl KeyTable {
    /// Builds the table for sources [0, `sources`) from the index's keys, as one base (with [`Keys::Unique`], a key's
    /// older sources are marked deleted): for bulk loads and for collections from before the key table. `ops` is what
    /// the table then accounts for of the log.
    pub fn build<K: Codec, U: Codec>(&self, snapshot: &Snapshot<K, U>, sources: u32, ops: u64) -> io::Result<Vec<u32>> {
        let mut order: Vec<(u64, u32)> = (0..sources).map(|o| (hash(&encoded(&snapshot.key(o))), o)).collect();
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
        let mut live = SourceSet::new(sources);
        let gone: HashSet<u32> = deleted.iter().copied().collect();
        for &(_, o) in &order {
            if !gone.contains(&o) {
                live.insert(o);
            }
        }
        let meta = self.meta();
        let generation = meta.generation + 1;
        let base_count = write_base(&self.files, generation, snapshot, &live)?;
        let mut txn = self.db.begin_write();
        for &o in &deleted {
            let mut entry = vec![DELETED];
            entry.extend_from_slice(&o.to_be_bytes());
            txn.put(&entry, &[])?;
        }
        let overlay: Vec<Vec<u8>> = self.db.begin_read().range_from(&[HASHED]).take_while(|(k, _)| k[0] == HASHED).map(|(k, _)| k.to_vec()).collect();
        for entry in overlay {
            txn.delete(&entry);
        }
        txn.put(META, &Meta { sources, ops, generation, base_count, overlay: 0 }.encode())?;
        txn.commit()?;
        if meta.base_count > 0 {
            self.files.remove(&base_name(meta.generation))?;
        }
        Ok(deleted)
    }
}
