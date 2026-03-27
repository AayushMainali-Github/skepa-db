# Storage And Recovery

This document describes the current persistence, WAL, and recovery behavior.

## On-Disk Layout

A database directory contains:

- `catalog.json`
- `wal.log`
- `tables/`
- `indexes/`

## Persistence Model

- Catalog metadata is stored in `catalog.json`.
- Table snapshots are stored as line-based row files under `tables/`.
- Index snapshots are stored as JSON files under `indexes/`.

Critical metadata and snapshot writes use temp-file replacement, not direct overwrite.

## Atomic Replacement

The engine currently uses:

1. write temp file
2. flush temp file
3. sync temp file
4. rename temp file into place

This is used for:

- catalog writes
- table snapshot writes
- index snapshot writes
- WAL truncation

## WAL Semantics

For autocommit DML:

1. append `BEGIN <txid>`
2. append `OP <txid> <sql>`
3. append `COMMIT <txid>`
4. flush and sync each append
5. persist affected table snapshots
6. checkpoint and truncate WAL

For explicit transactions:

1. stage operations in memory
2. on commit, append `BEGIN`
3. append all `OP` lines
4. append `COMMIT`
5. persist touched table snapshots
6. checkpoint and truncate WAL

## Recovery Behavior

On open:

1. initialize storage layout
2. load catalog
3. bootstrap table snapshots
4. replay committed WAL transactions
5. checkpoint current state
6. truncate WAL

Recovery rules:

- committed transactions are replayed
- uncommitted transactions are ignored
- explicitly rolled-back transactions are ignored
- invalid committed transactions that still violate deferred `no action` constraints are skipped
- a truncated final WAL tail line is ignored instead of aborting recovery

Recovery logs now emit:

- malformed catalog fallback messages
- WAL replay summary counts

## Manual Admin Checkpoint

The server admin surface exposes `POST /checkpoint`.

That endpoint:

1. checkpoints current table snapshots
2. truncates the WAL
3. returns success only if both complete

This is an operational control, not a different durability mode.

## Manual Admin Checkpoint

The server admin surface now exposes `POST /checkpoint`.

That endpoint:

1. checkpoints current table snapshots
2. truncates the WAL
3. returns success only if both complete

This is an operational control, not a different durability mode.

## Malformed Or Corrupt Inputs

Current fallback behavior:

- missing catalog file: start with empty catalog
- malformed catalog file during startup: log and fall back to empty catalog
- missing or malformed index snapshot: rebuild indexes from table rows
- invalid index snapshot entries: rebuild/heal indexes from table rows

## Crash Guarantees After Commit Response

Current guarantee level:

- WAL commit records are flushed and synced before the commit path returns
- table snapshots are then written and checkpointed
- after restart, committed WAL entries are replayed if checkpoint/truncation did not finish

This is a solid local durability story, but not the same as a production-grade database with manifest-based multi-file atomic checkpoints and formally specified fsync guarantees for every platform.

## Known Limits

- directory-level fsync semantics are not explicitly handled
- checkpoint is still a multi-file process, not a manifest-switch architecture
- corruption handling is pragmatic and local, not exhaustive across every possible partial-write pattern
- TLS is expected to be terminated outside the database server process
- TLS is expected to be terminated outside the database server process
