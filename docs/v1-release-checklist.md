# v1 Release Checklist

Use this checklist before tagging the first `v1` release.

## Compatibility

- confirm `api_version` is correct in `GET /version`
- confirm `storage_format_version` is correct in core and server metadata
- confirm `docs/compatibility.md` matches the actual HTTP and storage contract
- confirm `docs/upgrades.md` matches the supported upgrade procedure

## Validation

- run `cargo test --workspace`
- run `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- verify release workflow still builds:
  - `skepa_db_cli`
  - `skepa_db_server`

## Release Artifacts

- verify README install instructions match published artifact names
- verify both CLI and server binaries are listed in the release workflow
- verify the server config model in docs matches the current flags/env vars

## Release Notes

- prepare release notes before tagging
- include:
  - summary
  - HTTP API compatibility status
  - storage compatibility status
  - upgrade notes
  - operator action
- base the release notes on `.github/release-notes-template.md`

## Final Sanity Check

- smoke-check CLI embedded mode
- smoke-check server startup with `--data-dir` and `--default-database`
- smoke-check remote CLI against the server
- confirm admin/operator docs still reflect the current server behavior
