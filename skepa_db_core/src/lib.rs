use std::path::PathBuf;

#[derive(Debug)]
pub struct Database {
    path: PathBuf,
}

impl Database {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn execute(&mut self, _input: &str) -> Result<String,String>{
        Ok("Ok".to_string())
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    
}