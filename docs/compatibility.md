# Compatibility

This document defines the current compatibility policy for `skepa-db`.

## Stable Identity

`skepa-db` currently has two compatibility surfaces:

- HTTP server API compatibility
- on-disk storage format compatibility

These are versioned separately.

## HTTP API Versioning

The server exposes an explicit API version:

- current API version: `v1`

This value is returned by:

- `GET /version`
- `GET /config`

Policy:

- additive response fields may be introduced within the same API version
- existing endpoint meanings and stable error codes should not change incompatibly within the same API version
- breaking HTTP API changes require a new API version value

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

## Upgrade Compatibility

Current compatibility expectation:

- HTTP clients should treat `api_version` as the contract boundary
- storage tools and operators should treat `storage_format_version` as the on-disk contract boundary
- import/export is currently the conservative fallback for moving data between incompatible storage generations

## Current Limits

- there is not yet an automatic storage migration framework
- upgrade notes are manual and documented release-by-release
- downgrade support is not guaranteed across storage format changes
