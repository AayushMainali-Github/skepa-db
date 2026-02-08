use crate::types::datatype::DataType;
use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    Int(i64),
    BigInt(i128),
    Decimal(Decimal),
    VarChar(String),
    Text(String),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    Uuid(Uuid),
    Json(JsonValue),
    Blob(Vec<u8>),
}

pub fn parse_value(dtype: &DataType, token: &str) -> Result<Value, String>{
    match dtype {
        DataType::Bool => parse_bool(token).map(Value::Bool),
        DataType::Int => {
            let n: i64 = token.parse().map_err(|_| format!("Expected int but got '{token}'"))?;
            Ok(Value::Int(n))
        }
        DataType::BigInt => {
            let n: i128 = token
                .parse()
                .map_err(|_| format!("Expected bigint but got '{token}'"))?;
            Ok(Value::BigInt(n))
        }
        DataType::Decimal { precision, scale } => {
            let d = token
                .parse::<Decimal>()
                .map_err(|_| format!("Expected decimal but got '{token}'"))?;
            validate_decimal_bounds(&d, *precision, *scale)?;
            Ok(Value::Decimal(d))
        }
        DataType::VarChar(max) => {
            let len = token.chars().count();
            if len > *max {
                return Err(format!("Expected varchar({max}) but got length {len}"));
            }
            Ok(Value::VarChar(token.to_string()))
        }
        DataType::Text => Ok(Value::Text(token.to_string())),
        DataType::Date => {
            let d = NaiveDate::parse_from_str(token, "%Y-%m-%d")
                .map_err(|_| format!("Expected date YYYY-MM-DD but got '{token}'"))?;
            Ok(Value::Date(d))
        }
        DataType::Timestamp => {
            let ts = parse_timestamp(token)?;
            Ok(Value::Timestamp(ts))
        }
        DataType::Uuid => {
            let u = Uuid::parse_str(token).map_err(|_| format!("Expected uuid but got '{token}'"))?;
            Ok(Value::Uuid(u))
        }
        DataType::Json => {
            let j: JsonValue = serde_json::from_str(token)
                .map_err(|_| format!("Expected valid JSON but got '{token}'"))?;
            Ok(Value::Json(j))
        }
        DataType::Blob => {
            let raw = token.strip_prefix("0x").unwrap_or(token);
            let bytes = hex::decode(raw)
                .map_err(|_| format!("Expected hex blob (e.g. 0xDEADBEEF) but got '{token}'"))?;
            Ok(Value::Blob(bytes))
        }
    }
}

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::Decimal(d) => d.normalize().to_string(),
        Value::VarChar(s) => s.clone(),
        Value::Text(s) => s.clone(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::Timestamp(ts) => ts.format("%Y-%m-%d %H:%M:%S").to_string(),
        Value::Uuid(u) => u.to_string(),
        Value::Json(j) => j.to_string(),
        Value::Blob(b) => format!("0x{}", hex::encode_upper(b)),
    }
}

fn parse_bool(token: &str) -> Result<bool, String> {
    match token.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(format!("Expected bool but got '{token}'")),
    }
}

fn parse_timestamp(token: &str) -> Result<NaiveDateTime, String> {
    NaiveDateTime::parse_from_str(token, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(token, "%Y-%m-%dT%H:%M:%S"))
        .map_err(|_| format!("Expected timestamp 'YYYY-MM-DD HH:MM:SS' but got '{token}'"))
}

fn validate_decimal_bounds(d: &Decimal, precision: u32, scale: u32) -> Result<(), String> {
    let actual_scale = d.scale();
    if actual_scale > scale {
        return Err(format!(
            "Decimal scale {} exceeds allowed scale {}",
            actual_scale, scale
        ));
    }
    let s = d.abs().normalize().to_string();
    let digits = s.chars().filter(|c| c.is_ascii_digit()).count() as u32;
    if digits > precision {
        return Err(format!(
            "Decimal precision {} exceeds allowed precision {}",
            digits, precision
        ));
    }
    Ok(())
}
