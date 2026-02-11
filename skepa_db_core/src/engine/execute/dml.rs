fn handle_create(
    table: String,
    columns: Vec<ColumnDef>,
    table_constraints: Vec<TableConstraintDef>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.create_table(table.clone(), columns, table_constraints)?;
    storage.create_table(&table)?;
    Ok(format!("created table {}", table))
}

fn handle_insert(
    table: String,
    values: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;

    if values.len() != schema.column_count() {
        return Err(format!(
            "Expected {} values but got {}",
            schema.column_count(),
            values.len()
        ));
    }

    let mut row: Row = Vec::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let token = &values[i];
        if col.not_null && token.eq_ignore_ascii_case("null") {
            return Err(format!("Column '{}' is NOT NULL", col.name));
        }
        let value = parse_value(&col.dtype, token)?;
        row.push(value);
    }

    let rows = storage.scan(&table)?;

    if !schema.primary_key.is_empty()
        && storage
            .lookup_pk_conflict(&table, schema, &row, None)?
            .is_some()
    {
        return Err(format!(
            "PRIMARY KEY constraint violation on column(s) {}",
            schema.primary_key.join(",")
        ));
    }
    if let Some(cols) = storage.lookup_unique_conflict(&table, schema, &row, None)? {
        return Err(format!(
            "UNIQUE constraint violation on column(s) {}",
            cols.join(",")
        ));
    }

    validate_unique_constraints(schema, rows, &row, None)?;
    validate_outgoing_foreign_keys(catalog, storage, schema, &row)?;

    storage.insert_row(&table, row)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!("inserted 1 row into {}", table))
}

