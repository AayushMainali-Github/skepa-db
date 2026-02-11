use crate::parser::command::ForeignKeyAction;

pub(super) fn parse_foreign_key_action(
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

pub(super) fn parse_column_name_list(
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
