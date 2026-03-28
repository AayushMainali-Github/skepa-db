# Upgrades

This document describes the current upgrade and release expectations for `skepa-db`.

## Before Upgrading

1. Check the release notes for the target version.
2. Compare:
   - `api_version`
   - `storage_format_version`
3. Back up the database directory, or export named databases through the HTTP API.

Example checks:

```bash
curl http://127.0.0.1:8080/version
curl -H "Authorization: Bearer <token>" http://127.0.0.1:8080/config
```

Backup choices:

- stop the server and copy the configured `data_dir`
- or export named databases through the HTTP API before changing binaries

## Upgrade Rules

- If `api_version` is unchanged, existing HTTP clients should continue to work except for additive fields.
- If `storage_format_version` is unchanged, existing database directories are expected to open directly.
- If `storage_format_version` increases, treat that as a storage compatibility boundary.

Official short-term upgrade procedure:

1. Read the target release notes.
2. Check `api_version` and `storage_format_version` from the running server.
3. Create a backup or export the databases you care about.
4. Install the new server/CLI binaries.
5. If `storage_format_version` is unchanged, start the new server and open the existing data directory directly.
6. If `storage_format_version` changes or direct open fails on a version boundary, create a fresh target database and restore through export/import.

## Explicit Failure Expectations

Operators should expect:

- newer unsupported storage metadata is rejected explicitly on open
- unchanged storage format should continue to open directly
- import/export is the conservative fallback if compatibility is uncertain
- if direct open is blocked by a storage-format boundary, import/export is the supported recovery path

## When Storage Format Changes

Current policy:

- newer binaries may reject opening databases written by a future unsupported storage format
- automatic in-place migrations are not implemented yet
- the current migration hook framework exists only to make version decisions explicit
- use backup/restore or import/export as the conservative migration path

Export/import migration example:

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8080/databases/analytics/export > analytics-export.json

curl -X POST \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8080/databases/analytics-restored/import \
  --data @analytics-export.json
```

## Downgrades

Current downgrade policy:

- downgrades are only expected to be safe when `storage_format_version` has not crossed a compatibility boundary
- after any future storage format bump, assume downgrade is unsafe unless release notes say otherwise
- use backup/restore or export/import instead of assuming downgrade support

## Release Notes Expectation

Each release should call out:

- HTTP API compatibility status
- storage format compatibility status
- any required operator action
- any known upgrade caveats

## Current State

`skepa-db` now exposes explicit compatibility metadata and a minimal storage migration hook, but it does not yet provide automatic in-place migration tooling. Operators should treat upgrades as managed changes and keep backups.
