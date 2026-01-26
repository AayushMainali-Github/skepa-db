#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Int,
    Text,
}

pub fn parse_datatype(s: &str) -> Result<DataType, String> {
    match s.to_lowercase().as_str() {
        "int" => Ok(DataType::Int),
        "text" => Ok(DataType::Text),
        other => Err(format!("Unknown type '{other}'. Use int|text")),
    }
}