# invertdb-collection

The collection layer: what every invertdb service did for itself around an [introvert](https://github.com/invertdb/introvert)
index, done once. A collection logs writes, commits them in batches and publishes each commit as a generation that
readers share; it adds deletes and upserts, which no introvert-based service had.

```rust
let collection = Collection::open(&files, MyDomain::open(&files)?)?;
collection.add(records)?;               // durable when it returns (write-ahead log, synced)
collection.delete(vec![key])?;          // durable too
collection.commit()?;                   // applied and published
let g = collection.current();           // one commit: g.snapshot, g.side, g.is_live(source)
```

A service implements `Domain`: how a record is logged, its key, and what a commit does with a batch (write side
data, such as vectors or coordinates, and add each record to the index). Everything else is here: the log, the
pending batch, commit order, publishing, recovery, and the key table.

## How it works

- **Deletes** mark a source's ordinal deleted; the index is never rewritten. Queries skip deleted sources
  (`Generation::is_live`). With `Keys::Unique` (the default), adding a live key replaces it; with `Keys::Multiple`, a
  key can name several sources (audio's recordings) and a delete removes them all; with `Keys::Ordinal`, a source is
  named by its ordinal (log lines, numbered as they arrive), so no key table is kept and a delete names one source.
- **Durability** is the domain's: `Durability::Full` (the default) syncs the log before an add or delete returns;
  `Durability::Process` writes it to the operating system only, for a domain whose own files aren't synced either
  (logs). `add_numbered` returns the first record's source ordinal.
- **The key table** doesn't store keys again (the index's source table has them): it finds a key's sources by a hash
  of the key and checks each candidate's key in the index. Its base is a blob of (hash prefix, ordinal) pairs sorted
  by prefix, 8 bytes a source, found by interpolation search; sources added since are hash entries in an overlay held
  in memory and rebuilt from the index at open. The overlay is merged into a new base, without deleted sources, once
  it outgrows a quarter of the old one. The deleted set and the counts are in a small
  [b-positive](https://github.com/invertdb/b-positive) table. Storing each key in b-positive instead took 32 bytes a
  source (432 MB for geo's 13.5M places, more than its whole index); a b-positive overlay of hash entries, 19, and
  made search's bulk load 23% slower.
- **Commit order:** the domain's side data and index adds (in log order), the index's commit, then the batch's key
  changes in one `keys.db` transaction that also records how many sources and log operations it accounts for, then
  publishing, then the log's reset ([blammo](https://github.com/invertdb/blammo)) with those two counts.
- **Recovery:** each logged operation is accounted for already (skipped), in the index but not the key table (a crash
  between the two commits: its key change is made now, in order), or pending again. A collection from before this
  layer (a log whose checkpoint is a source count, no key table) is opened as it was, and its keys are read from the
  index once.
- **Eager domains** (`Domain::eager`) apply records as they arrive instead of at commit, so a commit's records
  aren't all held (documents: only their keys wait for the commit), and commit early once the index buffers
  `Domain::flush_bytes`. Logged adds become sources in log order either way, so recovery is the same.
- **Bulk loads** (`load`) fill an empty collection in one commit without the log, in any order (vector sorts by cell,
  geo by Z-order); the keys are read from the index after.

- **Telemetry** (the `tracing` and `opentelemetry` API crates; nothing until a service installs a subscriber and a
  meter provider): spans `collection.open`, `.recover`, `.add`, `.delete`, `.load` and `.commit` (with `.apply`,
  `.seal` and `.keys` inside, and the index's and log's spans below them); metrics `invertdb.collection.sources`,
  `.deleted`, `.pending`, `.disk`, `.log` (gauges), `.commit.duration`, `.added`, `.removed`, `.operations`, each with
  the collection's name as `invertdb.collection`; and `Collection::stats` for `/_stats`.

## Tests

`cargo test`: adds, replacements and deletes in both key modes; a crash at each point of a commit; and 400 random
operations with crashes at random points, four times (two of them eager, flushing every few hundred bytes), against a model of what was
acknowledged (with the overlay folded every few
commits); a fold dropping deleted sources; interpolation search against a scan; bulk loads with duplicate keys; stats; and the metrics, read through an SDK meter provider (`tests/metrics.rs`). Each recovery branch and each part of a key lookup was checked by breaking
it: the tests fail for each.

Not yet: reclaiming deleted sources (a merge that rewrites the index without them).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this crate by you, as
defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
