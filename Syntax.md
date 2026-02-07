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

## Select
- Retrieves rows from the specified table.
- **Syntax**: select <table>
- **Example**: select users

## Select With Where
- Filters rows using a single WHERE condition.
- **Syntax**: select <table> where <column> <operator> <value>
- **Example**: select users where age gte 18

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


