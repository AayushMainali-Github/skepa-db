use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbConfig {
    pub path: PathBuf,
}

impl DbConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
