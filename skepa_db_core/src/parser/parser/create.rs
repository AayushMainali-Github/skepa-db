use crate::parser::command::{ColumnDef, Command, ForeignKeyAction, TableConstraintDef};
use crate::types::datatype::{DataType, parse_datatype};
use super::common::{parse_column_name_list, parse_foreign_key_action};

pub(super) fn parse_create(tokens: &[String]) -> Result<Command, String> {
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

pub(super) fn parse_drop(tokens: &[String]) -> Result<Command, String> {
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

pub(super) fn parse_datatype_in_create(
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

