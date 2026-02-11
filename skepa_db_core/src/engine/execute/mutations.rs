fn handle_update(
    table: String,
    assignments: Vec<Assignment>,
    filter: WhereClause,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;

    let mut compiled: Vec<(usize, Value)> = Vec::new();
    for a in assignments {
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == a.column)
            .ok_or_else(|| format!("Unknown column '{}' in UPDATE", a.column))?;
        if schema.columns[idx].not_null && a.value.eq_ignore_ascii_case("null") {
            return Err(format!("Column '{}' is NOT NULL", schema.columns[idx].name));
        }
        let dtype = &schema.columns[idx].dtype;
        let parsed = parse_value(dtype, &a.value)?;
        compiled.push((idx, parsed));
    }

    validate_where_columns(schema, &filter)?;
    let targeted_row_indices = if simple_eq_filter(&filter).is_some()
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &simple_eq_filter(&filter).expect("eq").0)
    {
        let (_, val) = simple_eq_filter(&filter).expect("eq");
        storage
            .lookup_pk_row_index(&table, schema, &val)?
            .map(|i| vec![i])
    } else if simple_eq_filter(&filter).is_some() {
        let (col, val) = simple_eq_filter(&filter).expect("eq");
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &col, &val)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &col, &val)?
        }
    } else {
        None
    };

    let (updated, new_rows, old_indices, old_rows) = {
        let rows = storage.scan(&table)?;
        let old_rows = rows.to_vec();
        let mut updated = 0usize;
        let mut new_rows = rows.to_vec();
        let old_indices: Vec<usize> = (0..rows.len()).collect();

        if let Some(indices) = targeted_row_indices {
            for i in indices {
                if i >= new_rows.len() {
                    continue;
                }
                let row = &mut new_rows[i];
                if eval_where_row(row, schema, &filter)? {
                    for (idx, new_value) in &compiled {
                        if let Some(slot) = row.get_mut(*idx) {
                            *slot = new_value.clone();
                        }
                    }
                    updated += 1;
                }
            }
        } else {
            for row in new_rows.iter_mut() {
                if eval_where_row(row, schema, &filter)? {
                    for (idx, new_value) in &compiled {
                        if let Some(slot) = row.get_mut(*idx) {
                            *slot = new_value.clone();
                        }
                    }
                    updated += 1;
                }
            }
        }

        validate_all_unique_constraints(schema, &new_rows)?;
        validate_all_foreign_keys(catalog, storage, schema, &new_rows)?;
        validate_restrict_on_parent_update(catalog, storage, &table, schema, &old_rows, &new_rows)?;
        (updated, new_rows, old_indices, old_rows)
    };
    storage.replace_rows_with_alignment(&table, new_rows, old_indices)?;
    let post_parent_rows = storage.scan(&table)?.to_vec();
    apply_on_update_cascade(catalog, storage, &table, schema, &old_rows, &post_parent_rows)?;
    storage.rebuild_indexes(&table, schema)?;

    Ok(format!("updated {} row(s) in {}", updated, table))
}

fn handle_delete(
    table: String,
    filter: WhereClause,
    catalog: &mut Catalog,
    storage: &mut dyn StorageEngine,
) -> Result<String, String> {
    let schema = catalog.schema(&table)?;
    validate_where_columns(schema, &filter)?;
    let targeted_row_indices = if simple_eq_filter(&filter).is_some()
        && schema.primary_key.len() == 1
        && schema.primary_key.first().is_some_and(|pk| pk == &simple_eq_filter(&filter).expect("eq").0)
    {
        let (_, val) = simple_eq_filter(&filter).expect("eq");
        storage
            .lookup_pk_row_index(&table, schema, &val)?
            .map(|i| vec![i])
    } else if simple_eq_filter(&filter).is_some() {
        let (col, val) = simple_eq_filter(&filter).expect("eq");
        if let Some(i) = storage.lookup_unique_row_index(&table, schema, &col, &val)? {
            Some(vec![i])
        } else {
            storage.lookup_secondary_row_indices(&table, schema, &col, &val)?
        }
    } else {
        None
    };

    let (deleted, kept_rows, kept_old_indices, deleted_rows) = {
        let rows = storage.scan(&table)?;

        let mut deleted = 0usize;
        let mut kept_rows: Vec<Row> = Vec::new();
        let mut kept_old_indices: Vec<usize> = Vec::new();
        let mut deleted_rows: Vec<Row> = Vec::new();
        if let Some(indices) = targeted_row_indices {
            let targets: std::collections::HashSet<usize> = indices.into_iter().collect();
            for (idx, row) in rows.iter().enumerate() {
                if !targets.contains(&idx) {
                    kept_rows.push(row.clone());
                    kept_old_indices.push(idx);
                    continue;
                }
                let should_delete = eval_where_row(row, schema, &filter)?;
                if should_delete {
                    validate_restrict_on_parent_delete(catalog, storage, &table, schema, row)?;
                    deleted += 1;
                    deleted_rows.push(row.clone());
                } else {
                    kept_rows.push(row.clone());
                    kept_old_indices.push(idx);
                }
            }
        } else {
            let mut keep_flags: Vec<bool> = Vec::with_capacity(rows.len());
            for row in rows.iter() {
                let should_delete = eval_where_row(row, schema, &filter)?;
                keep_flags.push(!should_delete);
            }

            for (idx, (row, keep)) in rows.iter().cloned().zip(keep_flags).enumerate() {
                if keep {
                    kept_rows.push(row);
                    kept_old_indices.push(idx);
                } else {
                    validate_restrict_on_parent_delete(catalog, storage, &table, schema, &rows[idx])?;
                    deleted += 1;
                    deleted_rows.push(rows[idx].clone());
                }
            }
        }
        (deleted, kept_rows, kept_old_indices, deleted_rows)
    };
    storage.replace_rows_with_alignment(&table, kept_rows, kept_old_indices)?;
    apply_on_delete_cascade(catalog, storage, &table, schema, &deleted_rows)?;
    storage.rebuild_indexes(&table, schema)?;

    Ok(format!("deleted {} row(s) from {}", deleted, table))
}

