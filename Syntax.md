# Syntax Details (Subject to Change)
## Create
- Creates a new table with specified columns and their data types.
- **Syntax**: create <table> <col>:<type> <col>:<type> ...
- **Example**: create users id:int name:text age:int

**Currently Supported Types:**
- int
- text

## Insert
- Inserts a new row into the specified table with given values.
- **Syntax**: insert <table> <val> <val> ...
- **Example**: insert users 1 "Alice" 30

## Update
- Updates one or more columns for rows that match a WHERE condition.
- **Syntax**: update <table> set <col> <value> [<col> <value> ...] where <column> <operator> <value>
- **Examples**:
  - update users set name "Ravi" where id = 1
  - update users set name "Ravi" age 25 where id eq 1

## Select
- Retrieves rows (all columns with `*`, or specific columns with comma list).
- **Syntax**: select <col1,col2|*> from <table>
- **Examples**:
  - select * from users
  - select id,name from users

## Select Specific Columns
- Retrieves only selected columns.
- **Syntax**: select <col1,col2|*> from <table>
- **Examples**:
  - select id,name from users
  - select * from users

## Select With Where
- Filters rows using a single WHERE condition.
- **Syntax**: select <col1,col2|*> from <table> where <column> <operator> <value>
- **Examples**:
  - select * from users where age gte 18
  - select name from users where age gte 18

### WHERE Operators
- Equality (int/text): `=` or `eq`
- Numeric only: `>` or `gt`, `<` or `lt`, `>=` or `gte`, `<=` or `lte`
- Text pattern matching only: `like`

### LIKE Pattern Matching (`*` and `?` wildcards)
- Starts with: `"ra*"`
- Ends with: `"*ir"`
- Contains: `"*av*"`
- Exact (no wildcard): `"ram"`
- Single-character wildcard (`?`): `"r?m"`, `"??vi"`


