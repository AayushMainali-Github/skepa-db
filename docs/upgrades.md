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

## When Storage Format Changes

Current policy:

- newer binaries may reject opening databases written by a future unsupported storage format
- automatic migrations are not implemented yet
- use backup/restore or import/export as the conservative migration path

## Release Notes Expectation

Each release should call out:

- HTTP API compatibility status
- storage format compatibility status
- any required operator action
- any known upgrade caveats

## Current State

`skepa-db` now exposes explicit compatibility metadata, but it does not yet provide automatic migration tooling. Operators should treat upgrades as managed changes and keep backups.
