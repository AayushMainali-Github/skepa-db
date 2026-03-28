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

## Compatibility Checklist

Before tagging a release:

1. Confirm `api_version`.
2. Confirm `storage_format_version`.
3. State whether either value changed.
4. State whether direct upgrade is expected to work.
5. State whether import/export is recommended instead.

## Current Policy

- additive API changes may ship under the same API version
- breaking API changes require a new API version
- breaking storage changes require a new storage format version
- if storage compatibility changes, release notes must say so explicitly
