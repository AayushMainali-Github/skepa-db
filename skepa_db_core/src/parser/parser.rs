use crate::parser::command::{
    AlterAction, Assignment, ColumnDef, Command, CompareOp, ForeignKeyAction, JoinClause, JoinType, LogicalOp,
    OrderBy, Predicate, TableConstraintDef, WhereClause,
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
        "drop" => parse_drop(&tokens),
        "alter" => parse_alter(&tokens),
        "insert" => parse_insert(&tokens),
        "update" => parse_update(&tokens),
        "delete" => parse_delete(&tokens),
        "select" => parse_select(&tokens),
        _ => Err(format!("Unknown command '{}'", tokens[0])),
    }
}

fn parse_alter(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("table") {
        return Err("Usage: alter table <table> ...".to_string());
    }
    let table = tokens[2].clone();
    let head = tokens[3].to_lowercase();
    let action = match head.as_str() {
        "add" => parse_alter_add(tokens)?,
        "drop" => parse_alter_drop(tokens)?,
        "alter" => parse_alter_column(tokens)?,
        _ => return Err("Usage: alter table <table> add|drop|alter ...".to_string()),
    };
    Ok(Command::Alter { table, action })
}

fn parse_alter_add(tokens: &[String]) -> Result<AlterAction, String> {
    if tokens.len() < 6 {
        return Err("Usage: alter table <table> add ...".to_string());
    }
    if tokens[4].eq_ignore_ascii_case("unique") {
        let (cols, next) = parse_column_name_list(tokens, 5, tokens.len())?;
        if next != tokens.len() {
            return Err("Bad ALTER TABLE ADD UNIQUE syntax".to_string());
        }
        return Ok(AlterAction::AddUnique(cols));
    }
    if tokens[4].eq_ignore_ascii_case("foreign") {
        if tokens.len() < 10 || !tokens[5].eq_ignore_ascii_case("key") {
            return Err("Bad ALTER TABLE ADD FOREIGN KEY syntax".to_string());
        }
        let (cols, after_cols) = parse_column_name_list(tokens, 6, tokens.len())?;
        if after_cols >= tokens.len() || !tokens[after_cols].eq_ignore_ascii_case("references") {
            return Err("Bad ALTER TABLE ADD FOREIGN KEY syntax. Missing REFERENCES".to_string());
        }
        if after_cols + 1 >= tokens.len() {
            return Err("Bad ALTER TABLE ADD FOREIGN KEY syntax. Missing parent table".to_string());
        }
        let ref_table = tokens[after_cols + 1].clone();
        let (ref_cols, mut next) = parse_column_name_list(tokens, after_cols + 2, tokens.len())?;
        let mut on_delete = ForeignKeyAction::Restrict;
        let mut on_update = ForeignKeyAction::Restrict;
        loop {
            if next + 1 < tokens.len()
                && tokens[next].eq_ignore_ascii_case("on")
                && tokens[next + 1].eq_ignore_ascii_case("delete")
            {
                let (action, consumed) =
                    parse_foreign_key_action(tokens, next + 2, tokens.len(), "DELETE")?;
                on_delete = action;
                next = next + 2 + consumed;
                continue;
            }
            if next + 1 < tokens.len()
                && tokens[next].eq_ignore_ascii_case("on")
                && tokens[next + 1].eq_ignore_ascii_case("update")
            {
                let (action, consumed) =
                    parse_foreign_key_action(tokens, next + 2, tokens.len(), "UPDATE")?;
                on_update = action;
                next = next + 2 + consumed;
                continue;
            }
            break;
        }
        if next != tokens.len() {
            return Err("Bad ALTER TABLE ADD FOREIGN KEY syntax".to_string());
        }
        return Ok(AlterAction::AddForeignKey {
            columns: cols,
            ref_table,
            ref_columns: ref_cols,
            on_delete,
            on_update,
        });
    }
    Err("ALTER TABLE ADD supports UNIQUE(...) or FOREIGN KEY(...) REFERENCES ...".to_string())
}

fn parse_alter_drop(tokens: &[String]) -> Result<AlterAction, String> {
    if tokens.len() < 6 {
        return Err("Usage: alter table <table> drop ...".to_string());
    }
    if tokens[4].eq_ignore_ascii_case("unique") {
        let (cols, next) = parse_column_name_list(tokens, 5, tokens.len())?;
        if next != tokens.len() {
            return Err("Bad ALTER TABLE DROP UNIQUE syntax".to_string());
        }
        return Ok(AlterAction::DropUnique(cols));
    }
    if tokens[4].eq_ignore_ascii_case("foreign") {
        if tokens.len() < 10 || !tokens[5].eq_ignore_ascii_case("key") {
            return Err("Bad ALTER TABLE DROP FOREIGN KEY syntax".to_string());
        }
        let (cols, after_cols) = parse_column_name_list(tokens, 6, tokens.len())?;
        if after_cols >= tokens.len() || !tokens[after_cols].eq_ignore_ascii_case("references") {
            return Err("Bad ALTER TABLE DROP FOREIGN KEY syntax. Missing REFERENCES".to_string());
        }
        if after_cols + 1 >= tokens.len() {
            return Err("Bad ALTER TABLE DROP FOREIGN KEY syntax. Missing parent table".to_string());
        }
        let ref_table = tokens[after_cols + 1].clone();
        let (ref_cols, next) = parse_column_name_list(tokens, after_cols + 2, tokens.len())?;
        if next != tokens.len() {
            return Err("Bad ALTER TABLE DROP FOREIGN KEY syntax".to_string());
        }
        return Ok(AlterAction::DropForeignKey {
            columns: cols,
            ref_table,
            ref_columns: ref_cols,
        });
    }
    Err("ALTER TABLE DROP supports UNIQUE(...) or FOREIGN KEY(...) REFERENCES ...".to_string())
}

fn parse_alter_column(tokens: &[String]) -> Result<AlterAction, String> {
    if tokens.len() < 9 || !tokens[4].eq_ignore_ascii_case("column") {
        return Err("Usage: alter table <table> alter column <col> set|drop not null".to_string());
    }
    let col = tokens[5].clone();
    if tokens[6].eq_ignore_ascii_case("set")
        && tokens[7].eq_ignore_ascii_case("not")
        && tokens[8].eq_ignore_ascii_case("null")
        && tokens.len() == 9
    {
        return Ok(AlterAction::SetNotNull(col));
    }
    if tokens[6].eq_ignore_ascii_case("drop")
        && tokens[7].eq_ignore_ascii_case("not")
        && tokens[8].eq_ignore_ascii_case("null")
        && tokens.len() == 9
    {
        return Ok(AlterAction::DropNotNull(col));
    }
    Err("Usage: alter table <table> alter column <col> set|drop not null".to_string())
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
    if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("index") {
        return parse_create_index(tokens);
    }
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

fn parse_drop(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() >= 2 && tokens[1].eq_ignore_ascii_case("index") {
        return parse_drop_index(tokens);
    }
    Err("Unknown command 'drop'".to_string())
}

fn parse_create_index(tokens: &[String]) -> Result<Command, String> {
    // create index on <table> (col[,col...])
    if tokens.len() < 7 || !tokens[2].eq_ignore_ascii_case("on") {
        return Err("Usage: create index on <table> (<col>, ...)".to_string());
    }
    let table = tokens[3].clone();
    let (cols, next) = parse_column_name_list(tokens, 4, tokens.len())?;
    if next != tokens.len() {
        return Err("Usage: create index on <table> (<col>, ...)".to_string());
    }
    Ok(Command::CreateIndex {
        table,
        columns: cols,
    })
}

fn parse_drop_index(tokens: &[String]) -> Result<Command, String> {
    // drop index on <table> (col[,col...])
    if tokens.len() < 7 || !tokens[2].eq_ignore_ascii_case("on") {
        return Err("Usage: drop index on <table> (<col>, ...)".to_string());
    }
    let table = tokens[3].clone();
    let (cols, next) = parse_column_name_list(tokens, 4, tokens.len())?;
    if next != tokens.len() {
        return Err("Usage: drop index on <table> (<col>, ...)".to_string());
    }
    Ok(Command::DropIndex {
        table,
        columns: cols,
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
    let filter = parse_where_clause(
        where_tokens,
        "Bad UPDATE WHERE clause. Use: where <expr>, e.g. col = 1, col is null, col in (1,2), with and/or and parentheses",
    )?;

    Ok(Command::Update {
        table,
        assignments,
        filter,
    })
}

fn parse_delete(tokens: &[String]) -> Result<Command, String> {
    // delete from <table> where <column> <op> <value>
    if tokens.len() < 6
        || !tokens[1].eq_ignore_ascii_case("from")
        || !tokens[3].eq_ignore_ascii_case("where")
    {
        return Err("Usage: delete from <table> where <expr>".to_string());
    }

    let filter = parse_where_clause(
        &tokens[4..],
        "Usage: delete from <table> where <expr>",
    )?;
    Ok(Command::Delete {
        table: tokens[2].clone(),
        filter,
    })
}

fn parse_select(tokens: &[String]) -> Result<Command, String> {
    parse_select_projection(tokens)
}

fn parse_select_projection(tokens: &[String]) -> Result<Command, String> {
    let mut distinct = false;
    let projection_start = if tokens.len() > 1 && tokens[1].eq_ignore_ascii_case("distinct") {
        distinct = true;
        2
    } else {
        1
    };
    let from_idx = tokens
        .iter()
        .position(|t| t.eq_ignore_ascii_case("from"))
        .ok_or_else(|| {
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string()
        })?;

    if tokens.len() < 4 || from_idx <= projection_start {
        return Err(
            "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>]".to_string(),
        );
    }

    let columns = parse_select_columns(&tokens[projection_start..from_idx])?;
    if from_idx + 1 >= tokens.len() {
        return Err("SELECT missing table name after FROM".to_string());
    }
    let table = tokens[from_idx + 1].clone();

    let mut i = from_idx + 2;
    let mut join: Option<JoinClause> = None;
    let mut filter: Option<WhereClause> = None;
    let mut group_by: Option<Vec<String>> = None;
    let mut having: Option<WhereClause> = None;
    let mut order_by: Option<OrderBy> = None;
    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    if i < tokens.len() && (tokens[i].eq_ignore_ascii_case("join") || tokens[i].eq_ignore_ascii_case("left")) {
        let (join_type, join_kw_idx) = if tokens[i].eq_ignore_ascii_case("left") {
            if i + 1 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("join") {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            (JoinType::Left, i + 1)
        } else {
            (JoinType::Inner, i)
        };
        if join_kw_idx + 5 >= tokens.len()
            || !tokens[join_kw_idx + 2].eq_ignore_ascii_case("on")
            || tokens[join_kw_idx + 4] != "="
        {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        join = Some(JoinClause {
            join_type,
            table: tokens[join_kw_idx + 1].clone(),
            left_column: tokens[join_kw_idx + 3].clone(),
            right_column: tokens[join_kw_idx + 5].clone(),
        });
        i = join_kw_idx + 6;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("where") {
        if i + 2 >= tokens.len() {
            return Err(
                "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        let where_end = find_where_end(tokens, i + 1)?;
        filter = Some(parse_where_clause(
            &tokens[i + 1..where_end],
            "Usage: select <col1,col2|*> from <table> [where <expr>] [order by <column> [asc|desc]] [limit <n>]",
        )?);
        i = where_end;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("group") {
        if i + 2 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("by") {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]".to_string(),
            );
        }
        let (grp, next_i) = parse_group_by_columns(tokens, i + 2)?;
        group_by = Some(grp);
        i = next_i;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("having") {
        if i + 2 >= tokens.len() {
            return Err(
                "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]".to_string(),
            );
        }
        let having_end = find_having_end(tokens, i + 1)?;
        let having_tokens = normalize_function_tokens(&tokens[i + 1..having_end])?;
        having = Some(parse_where_clause(
            &having_tokens,
            "Usage: select <col1,col2|*> from <table> [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>]",
        )?);
        i = having_end;
    }

    if i < tokens.len() && tokens[i].eq_ignore_ascii_case("order") {
        if i + 2 >= tokens.len() || !tokens[i + 1].eq_ignore_ascii_case("by") {
            return Err(
                "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
            );
        }
        let (ob, next_i) = parse_order_by_list(tokens, i + 2)?;
        order_by = Some(ob);
        i = next_i;
    }

    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("limit") {
            if limit.is_some() {
                return Err("LIMIT specified more than once".to_string());
            }
            if i + 1 >= tokens.len() {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            let n = tokens[i + 1]
                .parse::<usize>()
                .map_err(|_| "LIMIT must be a non-negative integer".to_string())?;
            limit = Some(n);
            i += 2;
            continue;
        }
        if tokens[i].eq_ignore_ascii_case("offset") {
            if offset.is_some() {
                return Err("OFFSET specified more than once".to_string());
            }
            if i + 1 >= tokens.len() {
                return Err(
                    "Usage: select <col1,col2|*> from <table> [where <column> <op> <value>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
                );
            }
            let n = tokens[i + 1]
                .parse::<usize>()
                .map_err(|_| "OFFSET must be a non-negative integer".to_string())?;
            offset = Some(n);
            i += 2;
            continue;
        }
        break;
    }

    if i != tokens.len() {
        return Err(
            "Usage: select <col1,col2|*> from <table> [join|left join <table2> on <left_col> = <right_col>] [where <expr>] [group by <col1,col2>] [having <expr>] [order by <column> [asc|desc]] [limit <n>] [offset <n>]".to_string(),
        );
    }

    Ok(Command::Select {
        table,
        distinct,
        join,
        columns: Some(columns),
        filter,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn parse_order_by_list(tokens: &[String], mut i: usize) -> Result<(OrderBy, usize), String> {
    let mut items: Vec<(String, bool)> = Vec::new();
    loop {
        if i >= tokens.len() {
            return Err("ORDER BY requires at least one column".to_string());
        }
        let col = if i + 3 < tokens.len() && tokens[i + 1] == "(" && tokens[i + 3] == ")" {
            let c = format!("{}({})", tokens[i], tokens[i + 2]);
            i += 4;
            c
        } else {
            let c = tokens[i].clone();
            i += 1;
            c
        };
        let mut asc = true;
        if i < tokens.len() {
            if tokens[i].eq_ignore_ascii_case("asc") {
                asc = true;
                i += 1;
            } else if tokens[i].eq_ignore_ascii_case("desc") {
                asc = false;
                i += 1;
            }
        }
        items.push((col, asc));
        if i < tokens.len() && tokens[i] == "," {
            i += 1;
            continue;
        }
        break;
    }
    let (first_col, first_asc) = items
        .first()
        .cloned()
        .ok_or_else(|| "ORDER BY requires at least one column".to_string())?;
    let then_by = if items.len() > 1 {
        items[1..].to_vec()
    } else {
        Vec::new()
    };
    Ok((
        OrderBy {
            column: first_col,
            asc: first_asc,
            then_by,
        },
        i,
    ))
}

fn parse_select_columns(tokens: &[String]) -> Result<Vec<String>, String> {
    if tokens.len() == 1 && tokens[0] == "*" {
        return Ok(Vec::new());
    }

    let mut columns: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        if tokens[i] == "," {
            return Err(
                "SELECT column list cannot be empty. Use '*' or comma-separated column names."
                    .to_string(),
            );
        }
        let mut expr = if i + 1 < tokens.len() && tokens[i + 1] == "(" {
            if i + 3 >= tokens.len() || tokens[i + 3] != ")" {
                return Err("Bad SELECT function syntax. Use fn(col) or fn(*)".to_string());
            }
            let e = format!("{}({})", tokens[i], tokens[i + 2]);
            i += 4;
            e
        } else {
            let e = tokens[i].clone();
            i += 1;
            e
        };
        if i < tokens.len() && tokens[i].eq_ignore_ascii_case("as") {
            if i + 1 >= tokens.len() || tokens[i + 1] == "," {
                return Err("Bad SELECT alias syntax. Use: <expr> as <alias>".to_string());
            }
            expr = format!("{expr} as {}", tokens[i + 1]);
            i += 2;
        }
        columns.push(expr);
        if i < tokens.len() {
            if tokens[i] != "," {
                return Err("Bad SELECT column list. Use comma-separated column names.".to_string());
            }
            i += 1;
            if i >= tokens.len() {
                return Err("SELECT column list cannot end with comma".to_string());
            }
        }
    }
    if columns.is_empty() {
        return Err("SELECT column list cannot be empty. Use '*' or comma-separated column names.".to_string());
    }

    Ok(columns)
}

fn parse_group_by_columns(tokens: &[String], mut i: usize) -> Result<(Vec<String>, usize), String> {
    let mut cols: Vec<String> = Vec::new();
    loop {
        if i >= tokens.len() {
            return Err("GROUP BY requires at least one column".to_string());
        }
        if tokens[i] == "," || tokens[i] == "(" || tokens[i] == ")" {
            return Err("Bad GROUP BY column list".to_string());
        }
        cols.push(tokens[i].clone());
        i += 1;
        if i < tokens.len() && tokens[i] == "," {
            i += 1;
            continue;
        }
        break;
    }
    Ok((cols, i))
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
        "in" => Ok(CompareOp::In),
        _ => Err(format!(
            "Unknown WHERE operator '{raw}'. Use =|eq|>|gt|<|lt|>=|gte|<=|lte|like|in"
        )),
    }
}

fn parse_where_clause(tokens: &[String], usage_msg: &str) -> Result<WhereClause, String> {
    let mut idx = 0usize;
    let expr = parse_or_expr(tokens, &mut idx, usage_msg)?;
    if idx != tokens.len() {
        return Err(usage_msg.to_string());
    }
    Ok(expr)
}

fn parse_or_expr(tokens: &[String], idx: &mut usize, usage_msg: &str) -> Result<WhereClause, String> {
    let mut left = parse_and_expr(tokens, idx, usage_msg)?;
    while *idx < tokens.len() && tokens[*idx].eq_ignore_ascii_case("or") {
        *idx += 1;
        let right = parse_and_expr(tokens, idx, usage_msg)?;
        left = WhereClause::Binary {
            left: Box::new(left),
            op: LogicalOp::Or,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_and_expr(tokens: &[String], idx: &mut usize, usage_msg: &str) -> Result<WhereClause, String> {
    let mut left = parse_primary_expr(tokens, idx, usage_msg)?;
    while *idx < tokens.len() && tokens[*idx].eq_ignore_ascii_case("and") {
        *idx += 1;
        let right = parse_primary_expr(tokens, idx, usage_msg)?;
        left = WhereClause::Binary {
            left: Box::new(left),
            op: LogicalOp::And,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_primary_expr(tokens: &[String], idx: &mut usize, usage_msg: &str) -> Result<WhereClause, String> {
    if *idx >= tokens.len() {
        return Err(usage_msg.to_string());
    }
    if tokens[*idx] == "(" {
        *idx += 1;
        let expr = parse_or_expr(tokens, idx, usage_msg)?;
        if *idx >= tokens.len() || tokens[*idx] != ")" {
            return Err(usage_msg.to_string());
        }
        *idx += 1;
        return Ok(expr);
    }
    parse_predicate(tokens, idx, usage_msg)
}

fn parse_predicate(tokens: &[String], idx: &mut usize, usage_msg: &str) -> Result<WhereClause, String> {
    if *idx + 2 < tokens.len()
        && tokens[*idx + 1].eq_ignore_ascii_case("is")
        && tokens[*idx + 2].eq_ignore_ascii_case("null")
    {
        let p = Predicate {
            column: tokens[*idx].clone(),
            op: CompareOp::IsNull,
            value: String::new(),
        };
        *idx += 3;
        return Ok(WhereClause::Predicate(p));
    }
    if *idx + 3 < tokens.len()
        && tokens[*idx + 1].eq_ignore_ascii_case("is")
        && tokens[*idx + 2].eq_ignore_ascii_case("not")
        && tokens[*idx + 3].eq_ignore_ascii_case("null")
    {
        let p = Predicate {
            column: tokens[*idx].clone(),
            op: CompareOp::IsNotNull,
            value: String::new(),
        };
        *idx += 4;
        return Ok(WhereClause::Predicate(p));
    }
    if *idx + 4 < tokens.len() && tokens[*idx + 1].eq_ignore_ascii_case("in") {
        if tokens[*idx + 2] != "(" {
            return Err(usage_msg.to_string());
        }
        let mut vals: Vec<String> = Vec::new();
        let mut i = *idx + 3;
        while i < tokens.len() {
            if tokens[i] == ")" {
                if vals.is_empty() {
                    return Err(usage_msg.to_string());
                }
                let p = Predicate {
                    column: tokens[*idx].clone(),
                    op: CompareOp::In,
                    value: vals.join("\u{1F}"),
                };
                *idx = i + 1;
                return Ok(WhereClause::Predicate(p));
            }
            vals.push(tokens[i].clone());
            i += 1;
            if i < tokens.len() {
                if tokens[i] == ")" {
                    continue;
                }
                if tokens[i] != "," {
                    return Err(usage_msg.to_string());
                }
                i += 1;
                if i >= tokens.len() || tokens[i] == ")" {
                    return Err(usage_msg.to_string());
                }
            }
        }
        return Err(usage_msg.to_string());
    }
    if *idx + 2 < tokens.len() {
        let op = parse_compare_op(&tokens[*idx + 1])?;
        let p = Predicate {
            column: tokens[*idx].clone(),
            op,
            value: tokens[*idx + 2].clone(),
        };
        *idx += 3;
        return Ok(WhereClause::Predicate(p));
    }
    Err(usage_msg.to_string())
}

fn find_where_end(tokens: &[String], start: usize) -> Result<usize, String> {
    let mut i = start;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("group")
            || tokens[i].eq_ignore_ascii_case("having")
            || tokens[i].eq_ignore_ascii_case("order")
            || tokens[i].eq_ignore_ascii_case("limit")
            || tokens[i].eq_ignore_ascii_case("offset")
        {
            return Ok(i);
        }
        i += 1;
    }
    Ok(tokens.len())
}

fn find_having_end(tokens: &[String], start: usize) -> Result<usize, String> {
    let mut i = start;
    while i < tokens.len() {
        if tokens[i].eq_ignore_ascii_case("order")
            || tokens[i].eq_ignore_ascii_case("limit")
            || tokens[i].eq_ignore_ascii_case("offset")
        {
            return Ok(i);
        }
        i += 1;
    }
    Ok(tokens.len())
}

fn normalize_function_tokens(tokens: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0usize;
    while i < tokens.len() {
        if i + 3 < tokens.len() && tokens[i + 1] == "(" && tokens[i + 3] == ")" {
            out.push(format!("{}({})", tokens[i], tokens[i + 2]));
            i += 4;
        } else {
            out.push(tokens[i].clone());
            i += 1;
        }
    }
    Ok(out)
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
        let mut on_update = ForeignKeyAction::Restrict;

        loop {
            if next + 1 < end
                && tokens[next].eq_ignore_ascii_case("on")
                && tokens[next + 1].eq_ignore_ascii_case("delete")
            {
                let (action, consumed) = parse_foreign_key_action(tokens, next + 2, end, "DELETE")?;
                on_delete = action;
                next = next + 2 + consumed;
                continue;
            }
            if next + 1 < end
                && tokens[next].eq_ignore_ascii_case("on")
                && tokens[next + 1].eq_ignore_ascii_case("update")
            {
                let (action, consumed) = parse_foreign_key_action(tokens, next + 2, end, "UPDATE")?;
                on_update = action;
                next = next + 2 + consumed;
                continue;
            }
            break;
        }
        return Ok((
            TableConstraintDef::ForeignKey {
                columns: cols,
                ref_table,
                ref_columns: ref_cols,
                on_delete,
                on_update,
            },
            next,
        ));
    }
    Err("Unknown table constraint".to_string())
}

fn parse_foreign_key_action(
    tokens: &[String],
    start: usize,
    end: usize,
    action_kind: &str,
) -> Result<(ForeignKeyAction, usize), String> {
    if start >= end {
        return Err(format!(
            "Unknown ON {action_kind} action ''. Use restrict|cascade|set null|no action"
        ));
    }

    let t0 = tokens[start].to_lowercase();
    match t0.as_str() {
        "restrict" => Ok((ForeignKeyAction::Restrict, 1)),
        "cascade" => Ok((ForeignKeyAction::Cascade, 1)),
        "set" => {
            if start + 1 < end && tokens[start + 1].eq_ignore_ascii_case("null") {
                Ok((ForeignKeyAction::SetNull, 2))
            } else {
                Err(format!(
                    "Unknown ON {action_kind} action 'set'. Use restrict|cascade|set null|no action"
                ))
            }
        }
        "no" => {
            if start + 1 < end && tokens[start + 1].eq_ignore_ascii_case("action") {
                Ok((ForeignKeyAction::NoAction, 2))
            } else {
                Err(format!(
                    "Unknown ON {action_kind} action 'no'. Use restrict|cascade|set null|no action"
                ))
            }
        }
        other => Err(format!(
            "Unknown ON {action_kind} action '{other}'. Use restrict|cascade|set null|no action"
        )),
    }
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
