fn encode_key_parts(parts: &[String]) -> String {
    // Stable ASCII tuple encoding: each part is length-prefixed.
    let mut out = String::new();
    for p in parts {
        out.push_str(&p.len().to_string());
        out.push(':');
        out.push_str(p);
        out.push(';');
    }
    out
}

fn unique_groups(schema: &Schema) -> Result<Vec<Vec<String>>, String> {
    let mut out: Vec<Vec<String>> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for cols in &schema.unique_constraints {
        let key = cols.join(",");
        if seen.insert(key) {
            out.push(cols.clone());
        }
    }
    for col in &schema.columns {
        if col.unique && !col.primary_key {
            if schema.columns.iter().any(|c| c.name == col.name) {
                let cols = vec![col.name.clone()];
                let key = cols.join(",");
                if seen.insert(key) {
                    out.push(cols);
                }
            } else {
                return Err("Internal schema error while building UNIQUE indexes".to_string());
            }
        }
    }
    Ok(out)
}

fn parse_row_id_prefix(token: &str) -> Option<u64> {
    if !token.starts_with('@') || !token.ends_with('|') {
        return None;
    }
    token[1..token.len() - 1].parse::<u64>().ok()
}

fn validate_snapshot_entries(
    entries: Vec<IndexEntry>,
    known_row_ids: &[u64],
) -> Result<BTreeMap<String, u64>, String> {
    let known: std::collections::HashSet<u64> = known_row_ids.iter().copied().collect();
    let mut out = BTreeMap::new();
    for e in entries {
        if !known.contains(&e.row_id) {
            return Err("Index entry row id is not present".to_string());
        }
        if out.insert(e.key, e.row_id).is_some() {
            return Err("Duplicate key in index snapshot".to_string());
        }
    }
    Ok(out)
}

fn validate_secondary_snapshot_entries(
    entries: Vec<SecondaryIndexEntry>,
    known_row_ids: &[u64],
) -> Result<BTreeMap<String, Vec<u64>>, String> {
    let known: std::collections::HashSet<u64> = known_row_ids.iter().copied().collect();
    let mut out: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for e in entries {
        if out.contains_key(&e.key) {
            return Err("Duplicate key in secondary index snapshot".to_string());
        }
        if e.row_ids.is_empty() {
            return Err("Secondary index entry has empty row id list".to_string());
        }
        for rid in &e.row_ids {
            if !known.contains(rid) {
                return Err("Secondary index entry row id is not present".to_string());
            }
        }
        out.insert(e.key, e.row_ids);
    }
    Ok(out)
}

fn encode_value(v: &Value) -> String {
    match v {
        Value::Null => "n:".to_string(),
        Value::Bool(b) => format!("o:{}", if *b { "1" } else { "0" }),
        Value::Int(n) => format!("i:{n}"),
        Value::BigInt(n) => format!("g:{n}"),
        Value::Decimal(d) => format!("m:{}", d.normalize()),
        Value::VarChar(s) => format!("t:{}", escape_text(s)),
        Value::Text(s) => format!("t:{}", escape_text(s)),
        Value::Date(d) => format!("d:{}", d.format("%Y-%m-%d")),
        Value::Timestamp(ts) => format!("s:{}", ts.format("%Y-%m-%d %H:%M:%S")),
        Value::Uuid(u) => format!("u:{u}"),
        Value::Json(j) => format!("j:{}", escape_text(&j.to_string())),
        Value::Blob(b) => format!("b:{}", hex::encode(b)),
    }
}

fn decode_token(token: &str, dtype: &DataType) -> Result<String, String> {
    let (prefix, raw) = token
        .split_once(':')
        .ok_or_else(|| format!("Malformed value token '{token}'"))?;
    if prefix == "n" {
        return Ok("null".to_string());
    }
    match dtype {
        DataType::Bool => {
            if prefix != "o" {
                return Err(format!("Expected bool token prefix 'o:' but got '{token}'"));
            }
            Ok(match raw {
                "1" => "true".to_string(),
                "0" => "false".to_string(),
                other => {
                    return Err(format!("Malformed bool payload '{other}' in token '{token}'"))
                }
            })
        }
        DataType::Int => {
            if prefix != "i" {
                return Err(format!("Expected int token prefix 'i:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::BigInt => {
            if prefix != "g" {
                return Err(format!("Expected bigint token prefix 'g:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Decimal { .. } => {
            if prefix != "m" {
                return Err(format!("Expected decimal token prefix 'm:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::VarChar(_) | DataType::Text => {
            if prefix != "t" {
                return Err(format!("Expected text token prefix 't:' but got '{token}'"));
            }
            unescape_text(raw)
        }
        DataType::Date => {
            if prefix != "d" {
                return Err(format!("Expected date token prefix 'd:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Timestamp => {
            if prefix != "s" {
                return Err(format!("Expected timestamp token prefix 's:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Uuid => {
            if prefix != "u" {
                return Err(format!("Expected uuid token prefix 'u:' but got '{token}'"));
            }
            Ok(raw.to_string())
        }
        DataType::Json => {
            if prefix != "j" {
                return Err(format!("Expected json token prefix 'j:' but got '{token}'"));
            }
            unescape_text(raw)
        }
        DataType::Blob => {
            if prefix != "b" {
                return Err(format!("Expected blob token prefix 'b:' but got '{token}'"));
            }
            Ok(format!("0x{}", raw))
        }
    }
}

fn escape_text(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn unescape_text(input: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(other) => return Err(format!("Unsupported escape sequence '\\{other}'")),
            None => return Err("Dangling escape at end of text token".to_string()),
        }
    }
    Ok(out)
}
