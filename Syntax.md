# Syntax Details (Subject to Change)

## Create
- Creates a new table with specified columns and data types.
- **Syntax**: `create table <table> (<col> <type> [primary key|unique|not null], ..., [primary key(<col,...>)], [unique(<col,...>)], [foreign key(<col,...>) references <table>(<col,...>) [on delete restrict|cascade|set null|no action] [on update restrict|cascade|set null|no action]])`
- **Examples**:
  - `create table users (id int primary key, name text not null, age int)`
  - `create table sessions (user_id int, device text, token text, primary key(user_id,device), unique(token))`
  - `create table orders (id int, user_id int, foreign key(user_id) references users(id))`
  - `create table order_items (id int, order_id int, foreign key(order_id) references orders(id) on delete cascade on update cascade)`
  - `create table sessions (id int, user_id int, foreign key(user_id) references users(id) on delete set null on update no action)`

## Alter
- Alters constraints on an existing table.
- **Syntax**:
  - `alter table <table> add unique(<col,...>)`
  - `alter table <table> drop unique(<col,...>)`
  - `alter table <table> add foreign key(<col,...>) references <table>(<col,...>) [on delete restrict|cascade|set null|no action] [on update restrict|cascade|set null|no action]`
  - `alter table <table> drop foreign key(<col,...>) references <table>(<col,...>)`
  - `alter table <table> alter column <col> set not null`
  - `alter table <table> alter column <col> drop not null`
- Notes:
  - `create/alter table` are auto-commit operations and are rejected inside active transactions.
  - `add unique(...)` and `add foreign key(...)` validate existing table rows.
  - `set not null` validates existing rows and fails if any row has `null` in that column.

## Index
- Creates/drops a secondary (non-unique) index.
- **Syntax**:
  - `create index on <table> (<col,...>)`
  - `drop index on <table> (<col,...>)`
- Notes:
  - Current planner uses single-column equality indexes for `select`, `update`, and `delete` where possible.
  - Index entries skip rows where indexed column values are `null`.

## Insert
- Inserts one row into a table.
- **Syntax**: `insert into <table> values (<val>, <val>, ...)`
- **Example**: `insert into users values (1, "Alice", 30)`

## Transactions
- **Syntax**:
  - `begin`
  - `commit`
  - `rollback`
- Notes:
  - `insert`, `update`, `delete` can be grouped in one transaction.
  - `create table` is auto-commit and is not allowed inside an active transaction.

## Update
- Updates one or more columns for rows matching a WHERE condition.
- **Syntax**: `update <table> set <col> = <value> [, <col> = <value> ...] where <column> <operator> <value>`
- **Examples**:
  - `update users set name = "Ravi" where id = 1`
  - `update users set name = "Ravi", age = 25 where id eq 1`

## Delete
- Deletes rows matching a WHERE condition.
- **Syntax**: `delete from <table> where <column> <operator> <value>`
- **Examples**:
  - `delete from users where id = 1`
  - `delete from users where name like "r?m"`

## Select
- Retrieves all or selected columns.
- **Syntax**: `select <col1,col2|*> from <table> [where <column> <operator> <value>] [order by <column> [asc|desc]] [limit <n>]`
- **Examples**:
  - `select * from users`
  - `select id,name from users`
  - `select name from users where age gte 18`
  - `select id,name from users order by age desc limit 10`

### WHERE Operators
- Equality (int/text): `=` or `eq`
- Numeric only: `>` or `gt`, `<` or `lt`, `>=` or `gte`, `<=` or `lte`
- Text pattern matching only: `like`

### LIKE Pattern Matching
- `*` matches zero or more characters
- `?` matches exactly one character
- Examples:
  - Starts with: `"ra*"`
  - Ends with: `"*ir"`
  - Contains: `"*av*"`
  - Exact: `"ram"`
  - Single-char: `"r?m"`, `"??vi"`

## Constraints
- Column constraints:
  - `primary key` (implies `not null`)
  - `unique`
  - `not null`
- Table constraints (composite):
  - `primary key(col1,col2,...)`
  - `unique(col1,col2,...)`
  - `foreign key(col1,col2,...) references parent_table(parent_col1,parent_col2,...) [on delete restrict|cascade|set null|no action] [on update restrict|cascade|set null|no action]`
- Rules:
  - Only one primary key constraint is allowed per table.
  - Composite primary key must be declared as table-level `primary key(...)`.
  - Foreign key referenced columns must be a parent `primary key` or `unique` constraint.
  - `on delete` defaults to `restrict` when omitted.
  - `on update` defaults to `restrict` when omitted.
  - `on delete cascade` deletes matching child rows when parent rows are deleted.
  - `on update cascade` rewrites matching child key values when parent key values are updated.
  - `on delete set null` sets child FK columns to `null` when parent rows are deleted.
  - `on update set null` sets child FK columns to `null` when referenced parent keys are updated.
  - `no action` currently behaves the same as immediate `restrict`.
  - If any FK child column is `null`, referential check is skipped for that row.
