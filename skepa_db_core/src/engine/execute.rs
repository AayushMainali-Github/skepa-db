use crate::parser::command::Command;
use crate::storage::{Catalog, StorageEngine};
use crate::types::Row;
use crate::types::value::parse_value;
use crate::engine::format::format_select;

/// Executes a parsed command against the catalog and storage engine
pub fn execute_command(
    cmd: Command,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    match cmd {
        Command::Create { table, columns } => handle_create(table, columns, catalog, storage),
        Command::Insert { table, values } => handle_insert(table, values, catalog, storage),
        Command::Select { table } => handle_select(table, catalog, storage),
    }
}

/// Handles CREATE TABLE command
fn handle_create(
    table: String,
    columns: Vec<(String, crate::types::datatype::DataType)>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    // Create schema in catalog
    catalog.create_table(table.clone(), columns)?;
    
    // Create table in storage
    storage.create_table(&table)?;
    
    Ok(format!("created table {}", table))
}

/// Handles INSERT command
fn handle_insert(
    table: String,
    values: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    // Get schema to validate against
    let schema = catalog.schema(&table)?;
    
    // Validate value count matches column count
    if values.len() != schema.column_count() {
        return Err(format!(
            "Expected {} values but got {}",
            schema.column_count(),
            values.len()
        ));
    }
    
    // Parse raw string values into typed Values
    let mut row: Row = Vec::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let token = &values[i];
        let value = parse_value(&col.dtype, token)?;
        row.push(value);
    }
    
    // Insert the row into storage
    storage.insert_row(&table, row)?;
    
    Ok(format!("inserted 1 row into {}", table))
}

/// Handles SELECT command
fn handle_select(
    table: String,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    // Get schema for column names
    let schema = catalog.schema(&table)?;
    
    // Get rows from storage
    let rows = storage.scan(&table)?;
    
    // Format and return result
    Ok(format_select(schema, rows))
}
