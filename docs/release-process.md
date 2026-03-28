# Release Process

This document defines the minimum release process for `skepa-db`.

## Release Outputs

Each tagged release should publish:

- CLI binaries
- server binaries
- release notes

## Required Release Notes Sections

Every release note should include:

- summary of changes
- HTTP API compatibility status
- storage format compatibility status
- upgrade notes
- operator action required, if any

Use `.github/release-notes-template.md` as the source template, and keep a concrete release-note draft under `docs/releases/` before tagging.

Use `.github/release-notes-template.md` as the source template, and keep a concrete release-note draft under `docs/releases/` before tagging.

## Compatibility Checklist

Before tagging a release:

1. Confirm `api_version`.
2. Confirm `storage_format_version`.
3. State whether either value changed.
4. State whether direct upgrade is expected to work.
5. State whether import/export is recommended instead.
6. Link to `docs/upgrades.md` if operators need a concrete procedure.
7. Prepare the matching checklist/release-note docs:
   - `docs/v1-release-checklist.md`
   - `docs/releases/<version>.md`
7. Prepare the matching checklist/release-note docs:
   - `docs/v1-release-checklist.md`
   - `docs/releases/<version>.md`

## Current Policy

- additive API changes may ship under the same API version
- breaking API changes require a new API version
- breaking storage changes require a new storage format version
- if storage compatibility changes, release notes must say so explicitly
