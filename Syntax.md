# Syntax Details (Subject to Change)
## Create 
- Creates a new table with specified columns and their data types.
- **Syntax**: create \<table\> \<col\>:\<type\> \<col\>:\<type\> ...
- **Example**: create users id:int name:text age:int

**Curretly Supported Types:**
- int
- text

## Insert
- Inserts a new row into the specified table with given values.
- **Syntax**: insert \<table\> \<val\> \<val\> ...
- **Example**: insert users 1 "Alice" 30

## Select
- Retrieves rows from the specified table 
- **Syntax**: select \<table\>
- **Example**: select users