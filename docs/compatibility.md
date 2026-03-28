# Compatibility

This document defines the current compatibility policy for `skepa-db`.

## Stable Identity

`skepa-db` currently has two compatibility surfaces:

- HTTP server API compatibility
- on-disk storage format compatibility

These are versioned separately.

For `v1`, the intended stable identity is:

- a single-node embedded database engine
- a single-node HTTP server with documented endpoints
- a disk-backed on-disk format guarded by `storage_format_version`

## HTTP API Versioning

The server exposes an explicit API version:

- current API version: `v1`

This value is returned by:

- `GET /version`
- `GET /config`

Policy:

- additive response fields may be introduced within the same API version
- additive request fields may be introduced within the same API version only when they are optional
- existing endpoint meanings and stable error codes should not change incompatibly within the same API version
- breaking HTTP API changes require a new API version value

Within `v1`, the following are considered part of the stable HTTP contract:

- endpoint paths documented in `docs/api.md`
- request body field meanings for documented endpoints
- response envelope shape
- stable error code names

The following are not promised as stable public contract surfaces:

- debug payload internals under `/debug/catalog`
- debug payload internals under `/debug/storage`
- log line wording

## Storage Format Versioning

The engine exposes an explicit storage format version:

- current storage format version: `1`

This value is:

- embedded into `catalog.json` as `format_version`
- returned by `GET /version`
- returned by `GET /config`

Policy:

- a database created with an older known storage format may be opened by newer code if the format is still supported
- a database with a newer storage format than the running binary supports is rejected on open
- breaking on-disk format changes require incrementing the storage format version

Within `v1`, the following are the storage compatibility promises:

- `catalog.json` carries an explicit `format_version`
- a supported older catalog format may load
- a newer unsupported catalog format is rejected explicitly
- import/export remains the conservative fallback when direct open compatibility is not guaranteed

## Upgrade Compatibility

Current compatibility expectation:

- HTTP clients should treat `api_version` as the contract boundary
- storage tools and operators should treat `storage_format_version` as the on-disk contract boundary
- import/export is currently the conservative fallback for moving data between incompatible storage generations

## Downgrade Policy

Current downgrade policy is conservative:

- downgrade is not guaranteed across storage format changes
- if a database has been opened or rewritten by a newer binary with a newer storage format, older binaries may reject it
- import/export or backup/restore should be treated as the safe downgrade path when compatibility is uncertain

## Additive Change Policy Within `v1`

Allowed without changing `api_version`:

- optional request fields
- additive response fields
- new endpoints outside the documented stable endpoint set only if they do not change existing endpoint behavior

Not allowed without changing `api_version`:

- removing documented fields
- changing field meaning incompatibly
- changing stable error code names
- repurposing existing endpoints incompatibly

## Current Limits

- there is not yet an automatic storage migration framework
- upgrade notes are manual and documented release-by-release
- downgrade support is not guaranteed across storage format changes
