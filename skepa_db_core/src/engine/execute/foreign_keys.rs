pub fn validate_no_action_constraints(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
) -> Result<(), String> {
    for (child_table, child_schema) in catalog.snapshot_tables() {
        if child_schema.foreign_keys.is_empty() {
            continue;
        }
        let child_rows = storage.scan(&child_table)?;
        for fk in &child_schema.foreign_keys {
            if !matches!(fk.on_delete, ForeignKeyAction::NoAction)
                && !matches!(fk.on_update, ForeignKeyAction::NoAction)
            {
                continue;
            }
            let parent_schema = catalog.schema(&fk.ref_table)?;
            let parent_rows = storage.scan(&fk.ref_table)?;
            let child_idxs = resolve_cols_to_idxs(&child_schema, &fk.columns)?;
            let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
            for cr in child_rows {
                if child_idxs
                    .iter()
                    .any(|i| matches!(cr.get(*i), Some(Value::Null)))
                {
                    continue;
                }
                let found = parent_rows
                    .iter()
                    .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
                if !found {
                    return Err(format!(
                        "FOREIGN KEY NO ACTION violation: '{}' references '{}'",
                        child_table, fk.ref_table
                    ));
                }
            }
        }
    }
    Ok(())
}

