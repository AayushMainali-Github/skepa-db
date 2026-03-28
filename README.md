# skepa-db

`skepa-db` is a lightweight single-node SQL-style database with:
- an embeddable core engine
- a CLI for embedded and remote use
- an HTTP server runtime

## What This Is

- A disk-backed database engine.
- A local database CLI you can run directly.
- A small HTTP database server for trusted-network or self-hosted use.
- Data is persisted on disk.
- SQL-like syntax for `create`, `insert`, `select`, `update`, `delete`, constraints, indexes, and transactions.

## What This Is Not (Yet)

- No built-in TLS termination.
- No users/roles/permissions model beyond a shared bearer token.
- No PostgreSQL/MySQL wire compatibility.
- No distributed clustering or replication.

## Install

## Option 1: Download Prebuilt Binary (Recommended)

1. Open GitHub Releases for this repo.
2. Download the binary for your OS:
   - Windows: `skepa_db_cli-windows-x86_64.exe`
   - Linux: `skepa_db_cli-linux-x86_64`
   - macOS (Apple Silicon): `skepa_db_cli-macos-aarch64`
3. Run it from terminal.

If you need another architecture (for example macOS Intel), build from source.

## Option 2: Build From Source

Requirements:
- Rust toolchain (stable)

Build:

```bash
cargo build --release -p skepa_db_cli -p skepa_db_server
```

Run:

```bash
cargo run -p skepa_db_cli
cargo run -p skepa_db_server -- --data-dir ./data --default-database default --addr 127.0.0.1:8080
```

## Start Using

Run the CLI:

```bash
./skepa_db_cli
```

Example session:

```sql
create table users (id int primary key, name text not null);
insert into users values (1, "ram");
select * from users;
begin;
update users set name = "ravi" where id = 1;
commit;
```

Use `help` in CLI to see command guidance.

Remote CLI example:

```bash
cargo run -p skepa_db_cli -- execute "select * from users" --remote http://127.0.0.1:8080
```

Server with bearer auth:

```bash
cargo run -p skepa_db_server -- --config ./server.json --tls-terminated
```

## Data Location

CLI-embedded mode stores files under the DB path you open directly.
The server stores named database directories under its configured data directory.
Keep those folders safe; they contain table data, catalog metadata, and WAL.

## Backup / Restore (Current)

- Backup: stop writes and copy the DB directory.
- Restore: replace DB directory with your backup copy.

## Syntax and Behavior Reference

Command syntax reference:

- `Syntax.md`

Implementation semantics and product behavior:

- `docs/sql-dialect.md`
- `docs/transactions.md`
- `docs/storage.md`
- `docs/api.md`
- `docs/compatibility.md`
- `docs/upgrades.md`
- `docs/performance.md`

`Syntax.md` describes supported command forms. The `docs/` files describe the current engine, transaction, storage, and API semantics as implemented.

For embedded library use, the intended stable core boundary is:

- `Database::open(DbConfig)`
- `Database::execute(&str) -> QueryResult`

Legacy string-rendering helpers still exist only as compatibility/testing shims and should not be treated as the primary API surface.

Compatibility and upgrade policy now live in:

- `docs/compatibility.md`
- `docs/upgrades.md`
- `docs/release-process.md`
- `docs/server-operations.md`

## Server Operations

Current HTTP server surface includes:

- `GET /health`
- `GET /version`
- `GET /config`
- `GET /metrics`
- `GET /debug/catalog`
- `GET /debug/storage`
- `POST /checkpoint`
- `POST /execute`
- `POST /batch`
- named database lifecycle/execute endpoints
- named database import/export endpoints
- session endpoints for transaction-scoped execution

When an auth token is configured, admin and query endpoints require `Authorization: Bearer <token>`. `GET /health` and `GET /version` stay public for liveness and version checks.

The server starts with a metadata banner in logs and performs a best-effort checkpoint across discovered databases when shutting down on `Ctrl+C`.

For moving data between compatible `skepa-db` servers, use the HTTP database export/import endpoints. They package the named database directory contents as JSON for conservative backup-style transfer.

TLS should currently be terminated by a reverse proxy or trusted ingress in front of `skepa_db_server`.

For a full operator guide, including deployment topology, directory layout, checkpoint/shutdown guidance, and admin endpoint intent, see:

- `docs/server-operations.md`

## Quality Gates

The project uses CI for:
- formatting (`cargo fmt --all -- --check`)
- lints (`cargo clippy --workspace --all-targets --all-features -- -D warnings`)
- tests (`cargo test --workspace`)
