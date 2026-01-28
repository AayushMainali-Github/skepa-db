pub mod schema;
pub mod catalog;
pub mod engine;
pub mod mem;

// Re-export main types for convenience
pub use schema::{Schema, Column};
pub use catalog::Catalog;
pub use engine::StorageEngine;
pub use mem::MemStorage;
