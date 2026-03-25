use super::common::{parse_column_name_list, parse_foreign_key_action};
use crate::parser::command::{AlterAction, Command, ForeignKeyAction};

pub(super) fn parse_alter(tokens: &[String]) -> Result<Command, String> {
    if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("table") {
        return Err("Usage: alter table <table> ...".to_string());
    }
    let table = tokens[2].clone();
    let head = tokens[3].to_lowercase();
    let action = match head.as_str() {
        "add" => parse_alter_add(tokens)?,
        "drop" => parse_alter_drop(tokens)?,
        "alter" => parse_alter_column(tokens)?,
        _ => {
            return Err(
                "ALTER TABLE supports: add unique(...), add foreign key(...) references ... , drop unique(...), drop foreign key(...) references ..., alter column <col> set not null, alter column <col> drop not null"
                    .to_string(),
            )
        }
    };
    Ok(Command::Alter { table, action })
}

fn parse_alter_add(tokens: &[String]) -> Result<AlterAction, String> {
    if tokens.len() < 6 {
        return Err(
            "ALTER TABLE ADD supports: unique(<col>, ...) or foreign key(<col>, ...) references <table>(<col>, ...)"
                .to_string(),
        );
    }
    if tokens[4].eq_ignore_ascii_case("unique") {
        let (cols, next) = parse_column_name_list(tokens, 5, tokens.len())?;
        if next != tokens.len() {
            return Err(
                "Bad ALTER TABLE ADD UNIQUE syntax. Use: alter table <table> add unique(<col>, ...)"
                    .to_string(),
            );
        }
        return Ok(AlterAction::AddUnique(cols));
    }
    if tokens[4].eq_ignore_ascii_case("foreign") {
        if tokens.len() < 10 || !tokens[5].eq_ignore_ascii_case("key") {
            return Err(
                "Bad ALTER TABLE ADD FOREIGN KEY syntax. Use: alter table <table> add foreign key(<col>, ...) references <table>(<col>, ...)"
                    .to_string(),
            );
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
            return Err(
                "Bad ALTER TABLE ADD FOREIGN KEY syntax. Supported tail options: on delete <action>, on update <action>"
                    .to_string(),
            );
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
        return Err(
            "ALTER TABLE DROP supports: unique(<col>, ...) or foreign key(<col>, ...) references <table>(<col>, ...)"
                .to_string(),
        );
    }
    if tokens[4].eq_ignore_ascii_case("unique") {
        let (cols, next) = parse_column_name_list(tokens, 5, tokens.len())?;
        if next != tokens.len() {
            return Err(
                "Bad ALTER TABLE DROP UNIQUE syntax. Use: alter table <table> drop unique(<col>, ...)"
                    .to_string(),
            );
        }
        return Ok(AlterAction::DropUnique(cols));
    }
    if tokens[4].eq_ignore_ascii_case("foreign") {
        if tokens.len() < 10 || !tokens[5].eq_ignore_ascii_case("key") {
            return Err(
                "Bad ALTER TABLE DROP FOREIGN KEY syntax. Use: alter table <table> drop foreign key(<col>, ...) references <table>(<col>, ...)"
                    .to_string(),
            );
        }
        let (cols, after_cols) = parse_column_name_list(tokens, 6, tokens.len())?;
        if after_cols >= tokens.len() || !tokens[after_cols].eq_ignore_ascii_case("references") {
            return Err("Bad ALTER TABLE DROP FOREIGN KEY syntax. Missing REFERENCES".to_string());
        }
        if after_cols + 1 >= tokens.len() {
            return Err(
                "Bad ALTER TABLE DROP FOREIGN KEY syntax. Missing parent table".to_string(),
            );
        }
        let ref_table = tokens[after_cols + 1].clone();
        let (ref_cols, next) = parse_column_name_list(tokens, after_cols + 2, tokens.len())?;
        if next != tokens.len() {
            return Err(
                "Bad ALTER TABLE DROP FOREIGN KEY syntax. Use: alter table <table> drop foreign key(<col>, ...) references <table>(<col>, ...)"
                    .to_string(),
            );
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
        return Err(
            "ALTER TABLE ALTER COLUMN supports: alter column <col> set not null or alter column <col> drop not null"
                .to_string(),
        );
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
    Err(
        "ALTER TABLE ALTER COLUMN supports: alter column <col> set not null or alter column <col> drop not null"
            .to_string(),
    )
}
