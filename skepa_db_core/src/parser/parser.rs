use crate::parser::command::{
    Assignment, ColumnDef, Command, CompareOp, ForeignKeyAction, TableConstraintDef, WhereClause,
};
use crate::types::datatype::{DataType, parse_datatype};

pub fn parse(input: &str) -> Result<Command, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err("Empty command".to_string());
    }

    let keyword = tokens[0].to_lowercase();

    match keyword.as_str() {
        "begin" => parse_begin(&tokens),
        "commit" => parse_commit(&tokens),
        "rollback" => parse_rollback(&tokens),
        "create" => parse_create(&tokens),
        "insert" => parse_insert(&tokens),
        "update" => parse_update(&tokens),
        "delete" => parse_delete(&tokens),
        "select" => parse_select(&tokens),
        _ => Err(format!("Unknown command '{}'", tokens[0])),
    }
}

fn parse_begin(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: begin".to_string());
    }
    Ok(Command::Begin)
}

fn parse_commit(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: commit".to_string());
    }
    Ok(Command::Commit)
}

fn parse_rollback(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() != 1 {
        return Err("Usage: rollback".to_string());
    }
    Ok(Command::Rollback)
}

fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut just_closed_quote = false;

    let mut it = input.chars().peekable();

    while let Some(ch) = it.next() {
        match ch {
            '"' => {
                if just_closed_quote {
                    return Err(
                        "Unexpected quote after closing quote. Add whitespace between tokens."
                            .to_string(),
                    );
                }

                if !in_quotes {
                    if !current.is_empty() {
                        return Err("Quote (\") cannot start in the middle of a token. Add whitespace before the quote."
                            .to_string());
                    }
                    in_quotes = true;
                } else {
                    in_quotes = false;
                    just_closed_quote = true;
                }
            }

            '\\' if in_quotes => {
                match it.peek().copied() {
                    Some('"') => {
                        it.next();
                        current.push('"');
                    }
                    Some('\\') => {
                        it.next();
                        current.push('\\');
                    }
                    _ => {
                        return Err("Invalid escape sequence in quotes. Use \\\" for a quote or \\\\ for a backslash."
                            .to_string());
                    }
                }
                just_closed_quote = false;
            }

            c if c.is_whitespace() && !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                    continue;
                }

                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }

            ',' | '(' | ')' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            }

            '>' | '<' | '=' | '!' if !in_quotes => {
                if just_closed_quote {
                    tokens.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                } else if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                if let Some('=') = it.peek().copied() {
                    let mut op = String::new();
                    op.push(ch);
                    op.push('=');
                    it.next();
                    tokens.push(op);
                } else {
                    tokens.push(ch.to_string());
                }
            }

            _ => {
                if just_closed_quote {
                    return Err("Characters found immediately after a closing quote. Add whitespace after the quoted string."
                        .to_string());
                }
                current.push(ch);
            }
        }
    }

    if in_quotes {
        return Err("Unclosed quote (\") in input".to_string());
    }

    if !current.is_empty() || just_closed_quote {
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_create(tokens: &[String]) -> Result<Command, String> {
    // create table <table> ( <col> <type> [, <col> <type> ...] )
    if tokens.len() < 7 {
        return Err("Usage: create table <table> (<col> <type>, ...)".to_string());
    }
    if !tokens[1].eq_ignore_ascii_case("table") {
        return Err("Usage: create table <table> (<col> <type>, ...)".to_string());
    }
    if tokens[3] != "(" || tokens[tokens.len() - 1] != ")" {
        return Err("CREATE requires parenthesized column definitions".to_string());
    }
    let table = tokens[2].clone();

    let mut cols: Vec<ColumnDef> = Vec::new();
    let mut table_constraints: Vec<TableConstraintDef> = Vec::new();
    let mut i = 4usize;
    let end = tokens.len() - 1;

    while i < end {
        if i >= end {
            return Err("Bad CREATE column list. Use: (id int, name text)".to_string());
        }
        if tokens[i].eq_ignore_ascii_case("primary")
            || tokens[i].eq_ignore_ascii_case("unique")
            || tokens[i].eq_ignore_ascii_case("foreign")
        {
            let (constraint, next_i) = parse_table_constraint_in_create(tokens, i, end)?;
            table_constraints.push(constraint);
            i = next_i;
        } else {
            let name = tokens[i].clone();
            i += 1;
            let (dtype, next_i) = parse_datatype_in_create(tokens, i, end)?;
            let (primary_key, unique, not_null, after_constraints) =
                parse_constraints_in_create(tokens, next_i, end)?;
            i = after_constraints;
            cols.push(ColumnDef {
                name,
                dtype,
                primary_key,
                unique,
                not_null,
            });
        }
        if i < end {
            if tokens[i] != "," {
                return Err("Bad CREATE column list. Columns must be comma-separated.".to_string());
            }
            i += 1;
            if i >= end {
                return Err("Bad CREATE column list. Trailing comma is not allowed.".to_string());
            }
        }
    }

    if cols.is_empty() {
        return Err("CREATE requires at least one column".to_string());
    }

    Ok(Command::Create {
        table,
        columns: cols,
        table_constraints,
    })
}

fn parse_insert(tokens: &[String]) -> Result<Command, String> {
    // insert into <table> values (<v1>, <v2>, ...)
    if tokens.len() < 7 {
        return Err("Usage: insert into <table> values (<v1>, <v2>, ...)".to_string());
    }
    if !tokens[1].eq_ignore_ascii_case("into")
        || !tokens[3].eq_ignore_ascii_case("values")
        || tokens[4] != "("
        || tokens[tokens.len() - 1] != ")"
    {
        return Err("Usage: insert into <table> values (<v1>, <v2>, ...)".to_string());
    }
    let table = tokens[2].clone();
    let mut values: Vec<String> = Vec::new();
    let mut i = 5usize;
    let end = tokens.len() - 1;

    while i < end {
        values.push(tokens[i].clone());
        i += 1;
        if i < end {
            if tokens[i] != "," {
                return Err("Bad INSERT values. Values must be comma-separated.".to_string());
            }
            i += 1;
        }
    }

    if values.is_empty() {
        return Err("INSERT requires at least one value".to_string());
    }

    Ok(Command::Insert { table, values })
}

fn parse_update(tokens: &[String]) -> Result<Command, String> {
    // update <table> set <col> = <val> [, <col> = <val> ...] where <col> <op> <val>
    if tokens.len() < 10 {
        return Err(
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string(),
        );
    }

    let table = tokens[1].clone();
    if !tokens[2].eq_ignore_ascii_case("set") {
        return Err(
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string(),
        );
    }

    let where_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("where"))
        .ok_or_else(|| {
            "Usage: update <table> set <col> = <value> [, <col> = <value> ...] where <column> <op> <value>"
                .to_string()
        })?;

    if where_idx <= 3 {
        return Err("UPDATE requires at least one assignment after SET".to_string());
    }

    let set_tokens = &tokens[3..where_idx];
    let mut assignments: Vec<Assignment> = Vec::new();
    let mut i = 0usize;
    while i < set_tokens.len() {
        if i + 2 >= set_tokens.len() {
            return Err("Bad UPDATE assignments. Use: col = value, col = value".to_string());
        }
        if set_tokens[i + 1] != "=" {
            return Err("Bad UPDATE assignments. Use: col = value, col = value".to_string());
        }
        assignments.push(Assignment {
            column: set_tokens[i].clone(),
            value: set_tokens[i + 2].clone(),
        });
        i += 3;
        if i < set_tokens.len() {
            if set_tokens[i] != "," {
                return Err("Bad UPDATE assignments. Use comma between assignments.".to_string());
            }
            i += 1;
        }
    }

    let where_tokens = &tokens[where_idx + 1..];
    if where_tokens.len() != 3 {
        return Err("Bad UPDATE WHERE clause. Use: where <column> <op> <value>".to_string());
    }

    let op = parse_compare_op(&where_tokens[1])?;
    let filter = WhereClause {
        column: where_tokens[0].clone(),
        op,
        value: where_tokens[2].clone(),
    };

    Ok(Command::Update {
        table,
        assignments,
        filter,
    })
}

fn parse_delete(tokens: &[String]) -> Result<Command, String> {
    // delete from <table> where <column> <op> <value>
    if tokens.len() != 7
        || !tokens[1].eq_ignore_ascii_case("from")
        || !tokens[3].eq_ignore_ascii_case("where")
    {
        return Err("Usage: delete from <table> where <column> <op> <value>".to_string());
    }

    let op = parse_compare_op(&tokens[5])?;
    Ok(Command::Delete {
        table: tokens[2].clone(),
        filter: WhereClause {
            column: tokens[4].clone(),
            op,
            value: tokens[6].clone(),
        },
    })
}

fn parse_select(tokens: &[String]) -> Result<Command, String> {
    parse_select_projection(tokens)
}

fn parse_select_projection(tokens: &[String]) -> Result<Command, String> {
    let from_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("from"))
        .ok_or_else(|| {
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string()
        })?;

    if tokens.len() < 4 || from_idx < 2 {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let columns = parse_select_columns(&tokens[1..from_idx])?;
    if from_idx + 1 >= tokens.len() {
        return Err("SELECT missing table name after FROM".to_string());
    }
    let table = tokens[from_idx + 1].clone();

    if from_idx + 2 == tokens.len() {
        return Ok(Command::Select {
            table,
            columns: Some(columns),
            filter: None,
        });
    }

    if from_idx + 5 >= tokens.len() || !tokens[from_idx + 2].eq_ignore_ascii_case("where") {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }
    if from_idx + 6 != tokens.len() {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let op = parse_compare_op(&tokens[from_idx + 4])?;
    Ok(Command::Select {
        table,
        columns: Some(columns),
        filter: Some(WhereClause {
            column: tokens[from_idx + 3].clone(),
            op,
            value: tokens[from_idx + 5].clone(),
        }),
    })
}

fn parse_select_columns(tokens: &[String]) -> Result<Vec<String>, String> {
    if tokens.len() == 1 && tokens[0] == "*" {
        return Ok(Vec::new());
    }

    let mut columns: Vec<String> = Vec::new();
    let mut expect_col = true;
    for tok in tokens {
        if expect_col {
            if tok == "," {
                return Err(
                    "SELECT column list cannot be empty. Use '*' or comma-separated column names."
                        .to_string(),
                );
            }
            columns.push(tok.clone());
            expect_col = false;
        } else if tok != "," {
            return Err("Bad SELECT column list. Use comma-separated column names.".to_string());
        } else {
            expect_col = true;
        }
    }

    if columns.is_empty() || expect_col {
        return Err("SELECT column list cannot be empty. Use '*' or comma-separated column names.".to_string());
    }

    Ok(columns)
}

fn parse_compare_op(raw: &str) -> Result<CompareOp, String> {
    match raw.to_lowercase().as_str() {
        "=" | "eq" => Ok(CompareOp::Eq),
        "!=" => Err("Operator '!=' is not supported yet. Use '=' for now.".to_string()),
        ">" | "gt" => Ok(CompareOp::Gt),
        "<" | "lt" => Ok(CompareOp::Lt),
        ">=" | "gte" => Ok(CompareOp::Gte),
        "<=" | "lte" => Ok(CompareOp::Lte),
        "like" => Ok(CompareOp::Like),
        _ => Err(format!(
            "Unknown WHERE operator '{raw}'. Use =|eq|>|gt|<|lt|>=|gte|<=|lte|like"
        )),
    }
}

fn parse_datatype_in_create(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(DataType, usize), String> {
    if start >= end {
        return Err("Missing datatype in CREATE column definition".to_string());
    }
    let t = tokens[start].to_lowercase();
    match t.as_str() {
        "varchar" => {
            if start + 3 >= end || tokens[start + 1] != "(" || tokens[start + 3] != ")" {
                return Err("Bad varchar type. Use varchar(n)".to_string());
            }
            let combined = format!("varchar({})", tokens[start + 2]);
            Ok((parse_datatype(&combined)?, start + 4))
        }
        "decimal" => {
            if start + 5 >= end
                || tokens[start + 1] != "("
                || tokens[start + 3] != ","
                || tokens[start + 5] != ")"
            {
                return Err("Bad decimal type. Use decimal(p,s)".to_string());
            }
            let combined = format!("decimal({},{})", tokens[start + 2], tokens[start + 4]);
            Ok((parse_datatype(&combined)?, start + 6))
        }
        _ => Ok((parse_datatype(&tokens[start])?, start + 1)),
    }
}

fn parse_constraints_in_create(
    tokens: &[String],
    mut i: usize,
    end: usize,
) -> Result<(bool, bool, bool, usize), String> {
    let mut primary_key = false;
    let mut unique = false;
    let mut not_null = false;

    while i < end && tokens[i] != "," {
        let t = tokens[i].to_lowercase();
        match t.as_str() {
            "primary" => {
                if i + 1 >= end || !tokens[i + 1].eq_ignore_ascii_case("key") {
                    return Err("Bad PRIMARY KEY constraint. Use 'primary key'".to_string());
                }
                primary_key = true;
                i += 2;
            }
            "unique" => {
                unique = true;
                i += 1;
            }
            "not" => {
                if i + 1 >= end || !tokens[i + 1].eq_ignore_ascii_case("null") {
                    return Err("Bad NOT NULL constraint. Use 'not null'".to_string());
                }
                not_null = true;
                i += 2;
            }
            other => return Err(format!("Unknown column constraint token '{other}'")),
        }
    }

    if primary_key {
        unique = true;
        not_null = true;
    }

    Ok((primary_key, unique, not_null, i))
}

fn parse_table_constraint_in_create(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(TableConstraintDef, usize), String> {
    if tokens[start].eq_ignore_ascii_case("primary") {
        if start + 1 >= end || !tokens[start + 1].eq_ignore_ascii_case("key") {
            return Err("Bad PRIMARY KEY constraint. Use primary key(col1,col2)".to_string());
        }
        let (cols, next) = parse_column_name_list(tokens, start + 2, end)?;
        return Ok((TableConstraintDef::PrimaryKey(cols), next));
    }
    if tokens[start].eq_ignore_ascii_case("unique") {
        let (cols, next) = parse_column_name_list(tokens, start + 1, end)?;
        return Ok((TableConstraintDef::Unique(cols), next));
    }
    if tokens[start].eq_ignore_ascii_case("foreign") {
        if start + 1 >= end || !tokens[start + 1].eq_ignore_ascii_case("key") {
            return Err("Bad FOREIGN KEY constraint. Use foreign key(col) references t(col)".to_string());
        }
        let (cols, after_cols) = parse_column_name_list(tokens, start + 2, end)?;
        if after_cols >= end || !tokens[after_cols].eq_ignore_ascii_case("references") {
            return Err("Bad FOREIGN KEY constraint. Missing REFERENCES".to_string());
        }
        if after_cols + 1 >= end {
            return Err("Bad FOREIGN KEY constraint. Missing parent table".to_string());
        }
        let ref_table = tokens[after_cols + 1].clone();
        let (ref_cols, mut next) = parse_column_name_list(tokens, after_cols + 2, end)?;
        let mut on_delete = ForeignKeyAction::Restrict;
        if next + 2 < end
            && tokens[next].eq_ignore_ascii_case("on")
            && tokens[next + 1].eq_ignore_ascii_case("delete")
        {
            on_delete = match tokens[next + 2].to_lowercase().as_str() {
                "restrict" => ForeignKeyAction::Restrict,
                "cascade" => ForeignKeyAction::Cascade,
                other => {
                    return Err(format!(
                        "Unknown ON DELETE action '{other}'. Use restrict|cascade"
                    ))
                }
            };
            next += 3;
        }
        return Ok((
            TableConstraintDef::ForeignKey {
                columns: cols,
                ref_table,
                ref_columns: ref_cols,
                on_delete,
            },
            next,
        ));
    }
    Err("Unknown table constraint".to_string())
}

fn parse_column_name_list(
    tokens: &[String],
    start: usize,
    end: usize,
) -> Result<(Vec<String>, usize), String> {
    if start >= end || tokens[start] != "(" {
        return Err("Constraint column list must start with '('".to_string());
    }
    let mut i = start + 1;
    let mut cols: Vec<String> = Vec::new();
    let mut expect_col = true;
    while i < end {
        if tokens[i] == ")" {
            if cols.is_empty() || expect_col {
                return Err("Constraint column list cannot be empty".to_string());
            }
            return Ok((cols, i + 1));
        }
        if expect_col {
            if tokens[i] == "," {
                return Err("Bad constraint column list".to_string());
            }
            cols.push(tokens[i].clone());
            expect_col = false;
        } else if tokens[i] != "," {
            return Err("Bad constraint column list, expected comma".to_string());
        } else {
            expect_col = true;
        }
        i += 1;
    }
    Err("Unclosed constraint column list".to_string())
}
