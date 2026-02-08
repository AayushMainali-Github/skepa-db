# Syntax Details (Subject to Change)

## Create
- Creates a new table with specified columns and data types.
- **Syntax**: `create table <table> (<col> <type> [primary key|unique|not null], ..., [primary key(<col,...>)], [unique(<col,...>)])`
- **Examples**:
  - `create table users (id int primary key, name text not null, age int)`
  - `create table sessions (user_id int, device text, token text, primary key(user_id,device), unique(token))`

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
- **Syntax**: `select <col1,col2|*> from <table> [where <column> <operator> <value>]`
- **Examples**:
  - `select * from users`
  - `select id,name from users`
  - `select name from users where age gte 18`

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
- Rules:
  - Only one primary key constraint is allowed per table.
  - Composite primary key must be declared as table-level `primary key(...)`.
