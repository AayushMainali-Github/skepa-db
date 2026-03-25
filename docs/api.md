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
- `POST /execute`
- `POST /batch`
- `POST /session`
- `DELETE /session/{id}`
- `POST /session/{id}/execute`

## Stateless And Session Endpoints

Stateless endpoints:

- `/execute`
- `/batch`

Session endpoints:

- `/session`
- `/session/{id}`
- `/session/{id}/execute`

Transaction commands are rejected on stateless endpoints and must use session execution.

## Request Shapes

`POST /execute`

```json
{
  "sql": "select * from users"
}
```

`POST /batch`

```json
{
  "statements": [
    "create table users (id int, name text)",
    "insert into users values (1, \"ram\")",
    "select * from users"
  ]
}
```

## Response Shapes

Success envelope:

```json
{
  "ok": true,
  "request_id": 1,
  "result": {
    "Select": {
      "schema": { "...": "..." },
      "rows": [[1, "ram"]],
      "stats": {
        "rows_returned": 1,
        "rows_affected": null
      }
    }
  }
}
```

Error envelope:

```json
{
  "ok": false,
  "request_id": 1,
  "error": {
    "message": "transaction commands require a session endpoint"
  }
}
```

## Current Error Model

- HTTP errors currently return `400 Bad Request` with a message body
- there is not yet a stable machine-readable error code taxonomy
- parser, execution, and validation errors are surfaced as text messages

## CLI Modes

The CLI supports:

- embedded mode
- remote mode against the HTTP server

Commands:

- `shell`
- `execute "<sql>"`

Remote mode uses the same structured `QueryResult` shape as embedded mode.
