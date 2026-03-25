# SQL Dialect

`skepa-db` is SQL-like, not SQL-standard. This document describes the behavior the engine currently implements.

## Supported Statements

- `create table`
- `alter table`
- `create index`
- `drop index`
- `insert`
- `update`
- `delete`
- `select`
- `begin`
- `commit`
- `rollback`

## Data Types

Supported column types:

- `bool`
- `int`
- `bigint`
- `decimal(p,s)`
- `varchar(n)`
- `text`
- `date`
- `timestamp`
- `uuid`
- `json`
- `blob`

Values are parsed against the target column type. There is no general implicit cross-type coercion at execution time.

## Type Coercion Rules

- `insert` and `update` values are parsed using the destination column datatype.
- `where` comparison values are parsed using the compared column datatype.
- Join keys must have the same datatype.
- `gt`, `lt`, `gte`, and `lte` are only valid for `int`, `bigint`, `decimal`, `date`, and `timestamp`.
- `like` is only valid for `text` and `varchar`.
- Aggregate type rules:
  - `count(...)` returns `bigint`
  - `sum(int)` returns `int`
  - `sum(bigint)` returns `bigint`
  - `sum(decimal)` returns `decimal`
  - `avg(int|bigint)` returns `decimal(38,6)`
  - `avg(decimal)` returns `decimal` with scale at least `6`
  - `min` and `max` return the input datatype

## Null Semantics

- `null` is a first-class value.
- `where col = null` compares by value and does not behave like SQL three-valued logic.
- `is null` and `is not null` are supported explicitly and are the clearest way to query nulls.
- `unique` constraints treat nulls as distinct:
  - single-column unique allows multiple nulls
  - composite unique allows multiple rows if any member of the unique tuple is null
- `count(col)` skips nulls.
- `count(*)` counts rows.
- `sum`, `avg`, `min`, and `max` skip nulls and return `null` when there are no non-null inputs.
- Join equality does not match null join keys.
- Left joins fill unmatched right-side columns with nulls.

## Ordering Rules

- `order by` supports one or more columns with `asc` or `desc`.
- Default direction is ascending.
- Ordering is datatype-specific and only compares like-typed values.
- Null ordering is fixed:
  - ascending: nulls sort first
  - descending: nulls sort last
- For joins, unqualified `order by col` is rejected when the column name is ambiguous.
- For non-grouped selects, `order by` may resolve a projected alias.
- For grouped selects, `order by` can refer to grouped output columns and aggregate aliases.

## Filtering Rules

Supported comparison operators in `where`:

- `=`
- `eq`
- `>`
- `gt`
- `<`
- `lt`
- `>=`
- `gte`
- `<=`
- `lte`
- `like`
- `in`
- `is null`
- `is not null`

Logical composition:

- `and`
- `or`
- parenthesized expressions

`like` uses glob-style wildcards, not SQL `%`/`_`:

- `*` matches zero or more characters
- `?` matches exactly one character

Examples:

- `"ra*"`
- `"*ir"`
- `"r?m"`

## Select Semantics

- Plain `select *` and projected `select a,b` are supported.
- `distinct` is supported for plain selects.
- `group by` and `having` are supported.
- Aggregates:
  - `count`
  - `sum`
  - `avg`
  - `min`
  - `max`
- `distinct` inside aggregates is supported except `distinct *`.
- `having` requires either `group by` or aggregate functions.
- `select *` cannot be used with grouped/aggregate output.
- Non-aggregated selected columns in grouped queries must appear in `group by`.

## Join Semantics

Supported joins:

- `join` (inner join)
- `left join`

Rules:

- `join ... on` must compare one column from each table.
- Join columns must have the same datatype.
- Unqualified join/filter/order references are rejected when ambiguous.
- Inner join returns only matching rows.
- Left join preserves left-table row order and emits null-filled right columns for unmatched rows.

## Constraint Timing

- `primary key`, `unique`, and `not null` are enforced immediately.
- Foreign key `restrict`, `cascade`, and `set null` effects happen during statement execution.
- Foreign key `no action` is deferred to transaction commit and to WAL recovery validation.
- Schema changes such as `create table`, `alter table`, `create index`, and `drop index` are auto-commit operations and are rejected inside an active transaction.

## Unsupported Or Non-Standard Syntax

Known unsupported or intentionally different behavior:

- `!=` is not supported
- SQL `%` and `_` wildcards are not used by `like`
- Nested transactions are not supported
- Full SQL type coercion and casting rules are not implemented
- SQL three-valued logic is not implemented
- `distinct *` inside aggregates is not supported
- `sum(*)`, `avg(*)`, and `min/max(*)` are not supported

The parser tries to return direct usage or “not supported yet” errors for unsupported syntax.
