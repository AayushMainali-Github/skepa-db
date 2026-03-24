use thiserror::Error;

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DbError {
    #[error("{0}")]
    Message(String),
}

impl From<String> for DbError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for DbError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}
