use std::path::PathBuf;

pub mod types;
pub mod parser;
pub mod storage;

use storage::{Catalog, MemStorage};

#[derive(Debug)]
pub struct Database {
    path: PathBuf,
    #[allow(dead_code)]
    catalog: Catalog,
    #[allow(dead_code)]
    storage: MemStorage,
}

impl Database {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            catalog: Catalog::new(),
            storage: MemStorage::new(),
        }
    }

    pub fn execute(&mut self, _input: &str) -> Result<String, String> {
        Ok("Ok".to_string())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}