# skepa-db (v0.1.x)

`skepa-db` is a lightweight SQL-style database you run locally from a CLI binary.

This `0.1.x` release is focused on:
- local disk-backed storage
- transactions
- constraints (including foreign keys)
- WAL-based recovery

It is not a network server database yet.

## What This Is

- A local database CLI you run directly.
- Data is persisted on disk.
- SQL-like syntax for `create`, `insert`, `select`, `update`, `delete`, constraints, indexes, and transactions.

## What This Is Not (Yet)

- No server process (`localhost:5432` style) in `0.1.x`.
- No users/roles/auth permissions yet.
- No multi-client network protocol yet.

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
cargo build --release -p skepa_db_cli
```

Run:

```bash
cargo run -p skepa_db_cli
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

## Data Location

Database files are stored under the DB path used by the CLI/runtime.
Keep that folder safe; it contains table data, catalog metadata, and WAL.

## Backup / Restore (Current)

- Backup: stop writes and copy the DB directory.
- Restore: replace DB directory with your backup copy.

## Syntax and Behavior Reference

All syntax and behavior details are documented in:

- `Syntax.md`

`Syntax.md` is the source of truth for supported SQL-like commands and semantics.

## Quality Gates

The project uses CI for:
- formatting (`cargo fmt --all -- --check`)
- lints (`cargo clippy --workspace --all-targets --all-features -- -D warnings`)
- tests (`cargo test --workspace`)
