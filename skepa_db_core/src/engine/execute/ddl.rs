fn handle_create_index(
    table: String,
    columns: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.add_secondary_index(&table, columns.clone())?;
    let schema = catalog.schema(&table)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!(
        "created index on {}({})",
        table,
        columns.join(",")
    ))
}

fn handle_drop_index(
    table: String,
    columns: Vec<String>,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    catalog.drop_secondary_index(&table, &columns)?;
    let schema = catalog.schema(&table)?;
    storage.rebuild_indexes(&table, schema)?;
    Ok(format!(
        "dropped index on {}({})",
        table,
        columns.join(",")
    ))
}

fn handle_alter(
    table: String,
    action: AlterAction,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let before = catalog.clone();
    let result = match action {
        AlterAction::AddUnique(cols) => {
            catalog.add_unique_constraint(&table, cols.clone())?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_all_unique_constraints(schema, rows)?;
            storage.rebuild_indexes(&table, schema)?;
            Ok(format!("altered table {}: added unique({})", table, cols.join(",")))
        }
        AlterAction::DropUnique(cols) => {
            catalog.drop_unique_constraint(&table, &cols)?;
            let schema = catalog.schema(&table)?;
            storage.rebuild_indexes(&table, schema)?;
            Ok(format!("altered table {}: dropped unique({})", table, cols.join(",")))
        }
        AlterAction::AddForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        } => {
            catalog.add_foreign_key_constraint(
                &table,
                ForeignKeyDef {
                    columns: columns.clone(),
                    ref_table: ref_table.clone(),
                    ref_columns: ref_columns.clone(),
                    on_delete,
                    on_update,
                },
            )?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_all_foreign_keys(catalog, storage, schema, rows)?;
            Ok(format!(
                "altered table {}: added foreign key({}) references {}({})",
                table,
                columns.join(","),
                ref_table,
                ref_columns.join(",")
            ))
        }
        AlterAction::DropForeignKey {
            columns,
            ref_table,
            ref_columns,
        } => {
            catalog.drop_foreign_key_constraint(&table, &columns, &ref_table, &ref_columns)?;
            Ok(format!(
                "altered table {}: dropped foreign key({}) references {}({})",
                table,
                columns.join(","),
                ref_table,
                ref_columns.join(",")
            ))
        }
        AlterAction::SetNotNull(col) => {
            catalog.set_not_null(&table, &col, true)?;
            let schema = catalog.schema(&table)?;
            let rows = storage.scan(&table)?;
            validate_not_null_columns(schema, rows)?;
            Ok(format!("altered table {}: set {} not null", table, col))
        }
        AlterAction::DropNotNull(col) => {
            catalog.set_not_null(&table, &col, false)?;
            Ok(format!("altered table {}: dropped not null on {}", table, col))
        }
    };
    if result.is_err() {
        *catalog = before;
    }
    result
}

