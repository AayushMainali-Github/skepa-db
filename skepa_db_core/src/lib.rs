use std::path::PathBuf;

pub mod types;
pub mod parser;
pub mod storage;
pub mod engine;

use storage::{Catalog, MemStorage};

#[derive(Debug)]
pub struct Database {
    path: PathBuf,
    catalog: Catalog,
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

    pub fn execute(&mut self, input: &str) -> Result<String, String> {
        let cmd = parser::parser::parse(input)?;
        engine::execute_command(cmd, &mut self.catalog, &mut self.storage)
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}