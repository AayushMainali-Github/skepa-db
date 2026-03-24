use serde::Serialize;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ExecutionStats {
    pub rows_returned: Option<usize>,
    pub rows_affected: Option<usize>,
}
