# Upgrades

This document describes the current upgrade and release expectations for `skepa-db`.

## Before Upgrading

1. Check the release notes for the target version.
2. Compare:
   - `api_version`
   - `storage_format_version`
3. Back up the database directory, or export named databases through the HTTP API.

## Upgrade Rules

- If `api_version` is unchanged, existing HTTP clients should continue to work except for additive fields.
- If `storage_format_version` is unchanged, existing database directories are expected to open directly.
- If `storage_format_version` increases, treat that as a storage compatibility boundary.

## Explicit Failure Expectations

Operators should expect:

- newer unsupported storage metadata is rejected explicitly on open
- unchanged storage format should continue to open directly
- import/export is the conservative fallback if compatibility is uncertain
- if direct open is blocked by a storage-format boundary, import/export is the supported recovery path

## When Storage Format Changes

Current policy:

- newer binaries may reject opening databases written by a future unsupported storage format
- automatic migrations are not implemented yet
- use backup/restore or import/export as the conservative migration path

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

`skepa-db` now exposes explicit compatibility metadata, but it does not yet provide automatic migration tooling. Operators should treat upgrades as managed changes and keep backups.
