# Server Operations

This document describes the intended self-hosting and day-2 operations story for `skepa_db_server`.

## Deployment Topology

Recommended shape:

1. run `skepa_db_server` on a trusted private interface or loopback
2. place a reverse proxy or trusted ingress in front of it
3. terminate TLS at that proxy/ingress
4. require `Authorization: Bearer <token>` on protected routes

Current expectations:

- `skepa_db_server` does not terminate TLS itself
- bearer auth is shared-token auth, not per-user auth
- `GET /health` and `GET /version` can remain public for liveness/version checks
- all query/admin routes should be treated as protected when the server is exposed beyond localhost

## Startup And Config

Preferred config model:

- `data_dir`
- `default_database`
- `addr`
- optional `auth_token`
- optional `tls_terminated`

Example:

```json
{
  "data_dir": "./data",
  "default_database": "default",
  "addr": "127.0.0.1:8080",
  "auth_token": "replace-me",
  "tls_terminated": true
}
```

Recommended startup:

```bash
cargo run -p skepa_db_server -- --config ./server.json
```

Compatibility shorthands:

- `--db-path`
- `SKEPA_DB_PATH`

Those are still supported, but `data_dir + default_database` is the intended operational shape.

## Directory Layout

Server data is stored under `data_dir` as one directory per database:

```text
data/
  default/
    catalog.json
    wal.log
    tables/
    indexes/
  analytics/
    catalog.json
    wal.log
    tables/
    indexes/
```

Operational guidance:

- back up the whole named database directory
- do not edit files inside these directories while the server is running
- use named database import/export for conservative transfer between compatible servers

## Admin Endpoints

Protected admin/debug surface:

- `GET /config`
  - returns effective config without exposing the raw auth token
- `GET /metrics`
  - returns lightweight request/session counters
- `GET /debug/catalog`
  - returns the persisted catalog snapshot currently on disk
- `GET /debug/storage`
  - returns a storage snapshot including table and WAL information
- `POST /checkpoint`
  - forces a checkpoint and WAL truncation through the engine

These routes are intended for operators, not general application traffic.

## Checkpoint Guidance

Use `POST /checkpoint` when you want an explicit persistence boundary, for example:

- before planned maintenance
- before copying a database directory
- after important write-heavy admin activity

Current checkpoint semantics:

- persist current table/index snapshots
- persist catalog state
- truncate WAL after checkpoint completes

Checkpoint is best-effort operational tooling, not a replacement for backups.

## Graceful Shutdown

Current shutdown behavior:

- the server listens for `Ctrl+C`
- on shutdown, it attempts a best-effort checkpoint across discovered databases in `data_dir`

Recommended operator behavior:

1. stop client traffic
2. trigger `POST /checkpoint` if you want an explicit boundary
3. stop the process cleanly
4. back up or move database directories only after shutdown completes

## Import And Export

Use:

- `GET /databases/{name}/export`
- `POST /databases/{name}/import`

Recommended uses:

- conservative backup-style transfer between compatible servers
- restore into a fresh named database
- migration fallback when direct open is blocked by a storage compatibility boundary

This is currently the supported operator-facing migration workflow.

## Trusted-Network Guidance

If the server is reachable from anything other than localhost:

- put it behind TLS termination
- configure bearer auth
- avoid exposing debug/admin routes to the open internet
- prefer binding the server to a private address

This is a single-node database service, not a hardened internet-edge appliance.
