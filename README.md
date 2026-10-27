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
  key can name several sources (audio's recordings) and a delete removes them all.
- **The key table** doesn't store keys again (the index's source table has them): it finds a key's sources by a hash
  of the key and checks each candidate's key in the index. Its base is a blob of ordinals sorted by key hash (4 bytes
  a source; bulk loads write it); sources added since are hash entries in a small
  [b-positive](https://github.com/invertdb/b-positive) overlay, folded into a new base once it outgrows the old one.
  The deleted set is in b-positive too. Storing each key in b-positive instead took 32 bytes a source (432 MB for
  geo's 13.5M places, more than its whole index); hash entries alone, 19.
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

## Tests

`cargo test`: adds, replacements and deletes in both key modes; a crash at each point of a commit; and 400 random
operations with crashes at random points, four times (two of them eager, flushing every few hundred bytes), against a model of what was
acknowledged (with the overlay folded every few
commits); bulk loads with duplicate keys. Each recovery branch and each part of a key lookup was checked by breaking
it: the tests fail for each.

Not yet: reclaiming deleted sources (a merge that rewrites the index without them).
