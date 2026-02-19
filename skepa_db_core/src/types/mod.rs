pub mod datatype;
pub mod value;

use value::Value;

/// A row is a vector of values, one per column
pub type Row = Vec<Value>;
