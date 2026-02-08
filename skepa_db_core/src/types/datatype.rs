#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Int,
    BigInt,
    Decimal { precision: u32, scale: u32 },
    VarChar(usize),
    Text,
    Date,
    Timestamp,
    Uuid,
    Json,
    Blob,
}

pub fn parse_datatype(s: &str) -> Result<DataType, String> {
    let lower = s.to_lowercase();
    match lower.as_str() {
        "bool" => Ok(DataType::Bool),
        "int" => Ok(DataType::Int),
        "bigint" => Ok(DataType::BigInt),
        "text" => Ok(DataType::Text),
        "date" => Ok(DataType::Date),
        "timestamp" => Ok(DataType::Timestamp),
        "uuid" => Ok(DataType::Uuid),
        "json" => Ok(DataType::Json),
        "blob" => Ok(DataType::Blob),
        _ => parse_parametric_type(&lower),
    }
}

fn parse_parametric_type(s: &str) -> Result<DataType, String> {
    if let Some(inner) = s.strip_prefix("varchar(").and_then(|x| x.strip_suffix(')')) {
        let n: usize = inner
            .parse()
            .map_err(|_| format!("Bad varchar size '{inner}'. Use varchar(n)"))?;
        if n == 0 {
            return Err("varchar(n) requires n > 0".to_string());
        }
        return Ok(DataType::VarChar(n));
    }

    if let Some(inner) = s.strip_prefix("decimal(").and_then(|x| x.strip_suffix(')')) {
        let mut parts = inner.split(',');
        let p = parts
            .next()
            .ok_or_else(|| "decimal requires precision and scale: decimal(p,s)".to_string())?
            .trim();
        let s_part = parts
            .next()
            .ok_or_else(|| "decimal requires precision and scale: decimal(p,s)".to_string())?
            .trim();
        if parts.next().is_some() {
            return Err("decimal requires exactly two params: decimal(p,s)".to_string());
        }

        let precision: u32 = p
            .parse()
            .map_err(|_| format!("Bad decimal precision '{p}'"))?;
        let scale: u32 = s_part
            .parse()
            .map_err(|_| format!("Bad decimal scale '{s_part}'"))?;

        if precision == 0 || precision > 38 {
            return Err("decimal precision must be between 1 and 38".to_string());
        }
        if scale > precision {
            return Err("decimal scale must be <= precision".to_string());
        }
        return Ok(DataType::Decimal { precision, scale });
    }

    Err(format!(
        "Unknown type '{s}'. Use bool|int|bigint|decimal(p,s)|varchar(n)|text|date|timestamp|uuid|json|blob"
    ))
}
