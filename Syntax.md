# Syntax Details (Subject to Change)

## Create
- Creates a new table with specified columns and data types.
- **Syntax**: `create table <table> (<col> <type>, <col> <type>, ...)`
- **Example**: `create table users (id int, name text, age int)`

## Insert
- Inserts one row into a table.
- **Syntax**: `insert into <table> values (<val>, <val>, ...)`
- **Example**: `insert into users values (1, "Alice", 30)`

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
