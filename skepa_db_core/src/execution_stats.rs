#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionStats {
    pub rows_returned: Option<usize>,
    pub rows_affected: Option<usize>,
}
