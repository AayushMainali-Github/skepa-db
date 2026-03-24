use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionStats {
    pub rows_returned: Option<usize>,
    pub rows_affected: Option<usize>,
}
