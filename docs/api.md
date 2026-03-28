# API

This document describes the current programmatic API layers.

## Core Engine API

Primary types:

- `DbConfig`
- `DbError`
- `DbResult<T>`
- `QueryResult`
- `ExecutionStats`
- `Database`

Canonical engine entry points:

- `Database::open(config)`
- `Database::execute(sql)`

`QueryResult` variants:

- `Select`
- `Mutation`
- `SchemaChange`
- `Transaction`

`Select` includes:

- `schema`
- `rows`
- `stats`

`Mutation` includes:

- `message`
- `rows_affected`
- `stats`

## HTTP Server API

Initial endpoints:

- `GET /health`
- `GET /version`
- `GET /config`
- `GET /metrics`
- `GET /debug/catalog`
- `GET /debug/storage`
- `POST /checkpoint`
- `POST /execute`
- `POST /batch`
- `POST /session`
- `DELETE /session/{id}`
- `POST /session/{id}/execute`

## Auth And Exposure

The server now supports optional bearer-token protection.

Config inputs:

- `--config <path>`
- `--data-dir <path>`
- `--default-database <name>`
- `--db-path <path>`
- `--addr <host:port>`
- `--auth-token <token>`
- `--tls-terminated`
- `SKEPA_DB_CONFIG`
- `SKEPA_DB_DATA_DIR`
- `SKEPA_DB_DEFAULT_DATABASE`
- `SKEPA_DB_PATH`
- `SKEPA_DB_ADDR`
- `SKEPA_DB_AUTH_TOKEN`
- `SKEPA_DB_TLS_TERMINATED`

`--db-path` and `SKEPA_DB_PATH` remain supported as compatibility shorthands for setting both the data directory and default database from a single path.

When `auth_token` is configured:

- protected endpoints require `Authorization: Bearer <token>`
- missing or invalid tokens return `401 Unauthorized`
- `GET /health` and `GET /version` remain public
- `GET /version` returns server name and package version metadata

Example server config file:

```json
{
  "data_dir": "./data",
  "default_database": "default",
  "addr": "127.0.0.1:8080",
  "auth_token": "replace-me",
  "tls_terminated": true
}
```

`tls_terminated` means TLS is expected to be handled by a reverse proxy or trusted ingress in front of `skepa_db_server`. The current server does not terminate TLS itself.

On process shutdown, the server listens for `Ctrl+C` and attempts a best-effort checkpoint across the discovered database directories under `data_dir` before exiting.

## Stateless And Session Endpoints

Stateless endpoints:

- `/config`
- `/metrics`
- `/debug/catalog`
- `/debug/storage`
- `/checkpoint`
- `/execute`
- `/batch`

Session endpoints:

- `/session`
- `/session/{id}`
- `/session/{id}/execute`

Transaction commands are rejected on stateless endpoints and must use session execution.

Admin/debug endpoints are stateless but protected when auth is enabled.

## Request Shapes

`POST /execute`

```json
{
  "sql": "select * from users",
  "timeout_ms": 250,
  "idempotency_key": "req-123"
}
```

`POST /batch`

```json
{
  "statements": [
    "create table users (id int, name text)",
    "insert into users values (1, \"ram\")",
    "select * from users"
  ],
  "timeout_ms": 500,
  "idempotency_key": "batch-123"
}
```

`timeout_ms` is optional. It bounds how long the server will wait on the request before returning a timeout error.

`idempotency_key` is optional. The server currently logs and echoes this semantic boundary for clients, but it does not yet deduplicate repeated writes automatically. Clients should treat it as a request identity hint for now.

## Curl Examples

Stateless query:

```bash
curl -X POST http://127.0.0.1:8080/execute \
  -H "content-type: application/json" \
  -d '{
    "sql": "select * from users",
    "timeout_ms": 250,
    "idempotency_key": "req-123"
  }'
```

Authorized stateless batch:

```bash
curl -X POST http://127.0.0.1:8080/batch \
  -H "authorization: Bearer replace-me" \
  -H "content-type: application/json" \
  -d '{
    "statements": [
      "create table users (id int primary key, name text)",
      "insert into users values (1, \"ram\")",
      "select * from users"
    ],
    "timeout_ms": 500,
    "idempotency_key": "batch-123"
  }'
```

Session transaction flow:

1. Create a session:

```bash
curl -X POST http://127.0.0.1:8080/session \
  -H "authorization: Bearer replace-me"
```

2. Execute transaction commands through that session:

```bash
curl -X POST http://127.0.0.1:8080/session/1/execute \
  -H "authorization: Bearer replace-me" \
  -H "content-type: application/json" \
  -d '{"sql":"begin"}'

curl -X POST http://127.0.0.1:8080/session/1/execute \
  -H "authorization: Bearer replace-me" \
  -H "content-type: application/json" \
  -d '{"sql":"insert into users values (1, \"ram\")"}'

curl -X POST http://127.0.0.1:8080/session/1/execute \
  -H "authorization: Bearer replace-me" \
  -H "content-type: application/json" \
  -d '{"sql":"commit"}'
```

## Response Shapes

Success envelope:

```json
{
  "ok": true,
  "request_id": 1,
  "result": {
    "type": "select",
    "schema": { "...": "..." },
    "rows": [[1, "ram"]],
    "stats": {
      "rows_returned": 1,
      "rows_affected": null
    }
  }
}
```

Result types currently emitted by the HTTP API:

- `select`
- `mutation`
- `schema_change`
- `transaction`

Error envelope:

```json
{
  "ok": false,
  "request_id": 1,
  "error": {
    "code": "TRANSACTION_REQUIRES_SESSION",
    "message": "transaction commands require a session endpoint"
  }
}
```

## Current Error Model

- auth failures return `401 Unauthorized` with `UNAUTHORIZED`
- request validation, parser, execution, timeout, and admin-operation failures currently return `400 Bad Request`
- machine-readable error codes now exist for app integrations

Current stable codes:

- `INVALID_REQUEST`
- `UNAUTHORIZED`
- `SQL_PARSE_ERROR`
- `TRANSACTION_REQUIRES_SESSION`
- `SESSION_NOT_FOUND`
- `SESSION_HAS_ACTIVE_TRANSACTION`
- `UNIQUE_VIOLATION`
- `NOT_NULL_VIOLATION`
- `FOREIGN_KEY_VIOLATION`
- `CONFLICT`
- `TIMEOUT`
- `EXECUTION_ERROR`

The HTTP status is intentionally coarse right now. Application code should key off `error.code`, not only the status.

## Integration Guidance

- Treat `request_id` as a server-issued trace handle for logs and support, not as a client idempotency token.
- For writes, prefer one statement per `/execute` request unless your application needs the partial progress semantics of `/batch`.
- Use `/session/{id}/execute` for any transaction flow; `begin`, `commit`, and `rollback` are intentionally rejected on stateless endpoints.
- If you set `timeout_ms`, the server may return `TIMEOUT` while the final write outcome is still uncertain. Check state before retrying mutations.
- If auth is enabled, send `Authorization: Bearer <token>` on every protected request.

## Admin Endpoint Notes

- `GET /config` returns effective server config without exposing the raw auth token
- `GET /metrics` returns lightweight operational counters such as issued request count and active session transaction count
- `GET /debug/catalog` returns the persisted catalog snapshot from disk
- `GET /debug/storage` returns a storage snapshot including table counts and WAL file info
- `POST /checkpoint` forces a checkpoint plus WAL truncation through the engine

## CLI Modes

The CLI supports:

- embedded mode
- remote mode against the HTTP server

Commands:

- `shell`
- `execute "<sql>"`

Remote mode uses the same structured `QueryResult` shape as embedded mode.

## Idempotency Guidance

- Safe read requests like `select ...` may be retried by clients.
- Write requests should include an `idempotency_key` when the client needs to correlate retries.
- The current server does not store or enforce idempotency keys yet, so clients should still assume repeated writes can execute more than once.
- If a request times out, clients should verify resulting state before retrying a write.
