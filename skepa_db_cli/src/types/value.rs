use crate::types::datatype::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Text(String),
}

pub fn parse_value(dtype: &DataType, token: &str) -> Result<Value, String>{
    match dtype {
        DataType::Int => {
            let n: i64 = token.parse().map_err(|_| format!("Expected int but got '{token}'"))?;
            Ok(Value::Int(n))
        }
        DataType::Text => Ok(Value::Text(token.to_string())),
    }
}

pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Text(s) => s.clone(),
    }
}