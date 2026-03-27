# Performance

This document records the current performance workflow and the first measured findings.

## Benchmark Harness

The core engine now has a Criterion benchmark suite in:

- `skepa_db_core/benches/core_workloads.rs`

Current workload coverage:

- indexed equality select
- full-scan select
- indexed update
- indexed delete followed by insert
- transaction begin/commit
- transaction update/commit

The suite currently runs against seeded tables with:

- `100` rows
- `1000` rows

## How To Run

Compile benchmarks:

```bash
cargo bench -p skepa_db_core --no-run
```

Run the full workload suite:

```bash
cargo bench -p skepa_db_core --bench core_workloads
```

Run a narrower target while iterating on one path:

```bash
cargo bench -p skepa_db_core --bench core_workloads indexed_eq_select -- --sample-size 10
```

## First Findings

The first benchmark pass showed that indexed equality lookup was still paying for a full table scan before using the index.

After routing indexed equality select through direct row access on the storage boundary, the measured `indexed_eq_select/100` workload improved from roughly:

- `19-21 µs`

to roughly:

- `3.8-4.2 µs`

This is an approximately `80%` improvement on that measured case.

## Current Suspected Hot Spots

The next areas most likely worth measuring and optimizing are:

- transaction begin/commit overhead from coarse snapshot cloning
- transaction commit with writes
- recovery replay cost from statement-based WAL application
- index rebuild cost after bulk row mutation
- string-heavy row deduplication and formatting paths

## Guidance

- Do not optimize without a benchmark or workload-specific reproduction.
- Prefer changing one hot path at a time and re-running the narrow benchmark first.
- If a change improves microbenchmarks but hurts correctness or clarity at the engine boundary, reject it.
