# Transactions

This document describes the transaction behavior currently implemented by `skepa-db`.

## Supported Commands

- `begin`
- `commit`
- `rollback`

Only one active transaction exists per `Database` instance.

On the server:

- global `/execute` and `/batch` are stateless
- transaction commands must use `/session/{id}/execute`
- each session owns its own `Database` handle

## Isolation Model

The current behavior is closest to:

- read-your-own-writes inside a transaction
- optimistic conflict detection at commit
- per-table conflict detection using on-disk table file version hashes

This is not a standard named SQL isolation level.

## Read Behavior Inside A Transaction

- A transaction starts with in-memory snapshots of catalog and storage.
- Reads inside the transaction see the transaction’s own uncommitted writes.
- Reads do not provide a stable historical snapshot relative to other processes for untouched tables in the SQL-standard sense.
- Conflicts are checked at commit for touched tables by comparing the current table file hash to the hash captured at `begin`.

## Write Behavior

- DML changes inside a transaction are staged in memory.
- WAL records are appended only on commit, not on every statement inside the transaction.
- `rollback` restores the catalog and storage snapshots captured at `begin`.

## Conflict Detection

On `commit`, for each touched table:

- the current on-disk table file is hashed
- that hash is compared to the value captured at `begin`
- if the file changed, commit fails with a transaction conflict
- the database instance reloads from disk after the conflict

Implications:

- concurrent writes to the same table from another database instance can cause commit failure
- writes to other tables do not cause conflict for untouched tables

## Constraint Timing

- `primary key`, `unique`, and `not null` are checked immediately
- referential `restrict`, `cascade`, and `set null` happen during statement execution
- referential `no action` is validated at commit
- if deferred `no action` validation fails at commit, the transaction is rolled back to the `begin` snapshot

## Unsupported Transaction Behavior

- nested transactions are rejected
- schema changes are rejected inside an active transaction
- savepoints are not supported
- transaction isolation level configuration is not supported

## Server Session Rules

- `POST /session` creates a session
- `POST /session/{id}/execute` executes SQL in that session
- `DELETE /session/{id}` fails if the session has an active transaction

This keeps transaction state session-bound instead of global.
