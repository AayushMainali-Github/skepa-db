pub mod catalog;
pub mod disk;
pub mod engine;
pub mod schema;

// Re-export main types for convenience
pub use catalog::Catalog;
pub use disk::DiskStorage;
pub use engine::StorageEngine;
pub use schema::{Column, Schema};
