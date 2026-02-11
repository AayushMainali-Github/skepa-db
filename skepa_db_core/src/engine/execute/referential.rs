fn validate_all_foreign_keys(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    schema: &Schema,
    rows: &[Row],
) -> Result<(), String> {
    for r in rows {
        validate_outgoing_foreign_keys(catalog, storage, schema, r)?;
    }
    Ok(())
}

fn validate_outgoing_foreign_keys(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    schema: &Schema,
    row: &Row,
) -> Result<(), String> {
    for fk in &schema.foreign_keys {
        let parent_schema = catalog.schema(&fk.ref_table)?;
        let child_idxs = resolve_cols_to_idxs(schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if child_idxs
            .iter()
            .any(|i| matches!(row.get(*i), Some(Value::Null)))
        {
            continue;
        }
        let found = fk_parent_exists(catalog, storage, &fk.ref_table, parent_schema, row, &child_idxs, &parent_idxs)?;
        if !found {
            return Err(format!(
                "FOREIGN KEY violation on ({}) references {}({})",
                fk.columns.join(","),
                fk.ref_table,
                fk.ref_columns.join(",")
            ));
        }
    }
    Ok(())
}

fn validate_restrict_on_parent_delete(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    parent_row: &Row,
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::Restrict {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
        if fk_child_references_parent(
            storage,
            &child_table,
            child_schema,
            parent_row,
            &child_idxs,
            &parent_idxs,
        )? {
            return Err(format!(
                "FOREIGN KEY RESTRICT violation: '{}' is referenced by '{}'",
                parent_table, child_table
            ));
        }
    }
    Ok(())
}

fn validate_restrict_on_parent_update(
    catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_rows: &[Row],
    new_rows: &[Row],
) -> Result<(), String> {
    for (old_r, new_r) in old_rows.iter().zip(new_rows.iter()) {
        if old_r == new_r {
            continue;
        }
        for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
            if fk.on_update != ForeignKeyAction::Restrict {
                continue;
            }
            let child_schema = catalog.schema(&child_table)?;
            let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
            let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;
            let was_referenced = fk_child_references_parent(
                storage,
                &child_table,
                child_schema,
                old_r,
                &child_idxs,
                &parent_idxs,
            )?;
            if was_referenced && !tuple_eq(old_r, &parent_idxs, new_r, &parent_idxs) {
                return Err(format!(
                    "FOREIGN KEY RESTRICT violation: '{}' is referenced by '{}'",
                    parent_table, child_table
                ));
            }
        }
    }
    Ok(())
}

fn incoming_foreign_keys(catalog: &Catalog, parent_table: &str) -> Vec<(String, ForeignKeyDef)> {
    let mut out = Vec::new();
    for (table, schema) in catalog.snapshot_tables() {
        for fk in schema.foreign_keys {
            if fk.ref_table == parent_table {
                out.push((table.clone(), fk));
            }
        }
    }
    out
}

fn apply_on_delete_cascade(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    deleted_parent_rows: &[Row],
) -> Result<(), String> {
    if deleted_parent_rows.is_empty() {
        return Ok(());
    }
    apply_on_delete_set_null(catalog, storage, parent_table, parent_schema, deleted_parent_rows)?;
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::Cascade {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        let mut keep_rows: Vec<Row> = Vec::new();
        let mut keep_old_indices: Vec<usize> = Vec::new();
        let mut deleted_child_rows: Vec<Row> = Vec::new();
        for (idx, cr) in child_rows.iter().enumerate() {
            let referenced = deleted_parent_rows
                .iter()
                .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
            if !referenced {
                keep_rows.push(cr.clone());
                keep_old_indices.push(idx);
            } else {
                validate_restrict_on_parent_delete(catalog, storage, &child_table, child_schema, cr)?;
                deleted_child_rows.push(cr.clone());
            }
        }
        storage.replace_rows_with_alignment(&child_table, keep_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
        apply_on_delete_cascade(catalog, storage, &child_table, child_schema, &deleted_child_rows)?;
    }
    Ok(())
}

fn apply_on_delete_set_null(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    deleted_parent_rows: &[Row],
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_delete != ForeignKeyAction::SetNull {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        for ci in &child_idxs {
            if child_schema.columns[*ci].not_null {
                return Err(format!(
                    "FOREIGN KEY SET NULL requires nullable child column '{}.{}'",
                    child_table, child_schema.columns[*ci].name
                ));
            }
        }

        let mut updated_child_rows = child_rows.to_vec();
        for cr in &mut updated_child_rows {
            let referenced = deleted_parent_rows
                .iter()
                .any(|pr| tuple_eq(cr, &child_idxs, pr, &parent_idxs));
            if referenced {
                for ci in &child_idxs {
                    cr[*ci] = Value::Null;
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn apply_on_update_cascade(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_parent_rows: &[Row],
    new_parent_rows: &[Row],
) -> Result<(), String> {
    if old_parent_rows.len() != new_parent_rows.len() {
        return Err("Internal error: parent row alignment mismatch during ON UPDATE CASCADE".to_string());
    }
    apply_on_update_set_null(catalog, storage, parent_table, parent_schema, old_parent_rows, new_parent_rows)?;
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_update != ForeignKeyAction::Cascade {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let old_child_rows = child_rows.to_vec();
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        let mut updated_child_rows = old_child_rows.clone();
        for cr in &mut updated_child_rows {
            for (old_pr, new_pr) in old_parent_rows.iter().zip(new_parent_rows.iter()) {
                if tuple_eq(cr, &child_idxs, old_pr, &parent_idxs)
                    && !tuple_eq(old_pr, &parent_idxs, new_pr, &parent_idxs)
                {
                    for (ci, pi) in child_idxs.iter().zip(parent_idxs.iter()) {
                        cr[*ci] = new_pr[*pi].clone();
                    }
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        let post_child_rows = storage.scan(&child_table)?.to_vec();
        apply_on_update_cascade(
            catalog,
            storage,
            &child_table,
            child_schema,
            &old_child_rows,
            &post_child_rows,
        )?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn apply_on_update_set_null(
    catalog: &Catalog,
    storage: &mut dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    old_parent_rows: &[Row],
    new_parent_rows: &[Row],
) -> Result<(), String> {
    for (child_table, fk) in incoming_foreign_keys(catalog, parent_table) {
        if fk.on_update != ForeignKeyAction::SetNull {
            continue;
        }
        let child_schema = catalog.schema(&child_table)?;
        let child_rows = storage.scan(&child_table)?;
        let child_idxs = resolve_cols_to_idxs(child_schema, &fk.columns)?;
        let parent_idxs = resolve_cols_to_idxs(parent_schema, &fk.ref_columns)?;

        for ci in &child_idxs {
            if child_schema.columns[*ci].not_null {
                return Err(format!(
                    "FOREIGN KEY SET NULL requires nullable child column '{}.{}'",
                    child_table, child_schema.columns[*ci].name
                ));
            }
        }

        let mut updated_child_rows = child_rows.to_vec();
        for cr in &mut updated_child_rows {
            for (old_pr, new_pr) in old_parent_rows.iter().zip(new_parent_rows.iter()) {
                if tuple_eq(cr, &child_idxs, old_pr, &parent_idxs)
                    && !tuple_eq(old_pr, &parent_idxs, new_pr, &parent_idxs)
                {
                    for ci in &child_idxs {
                        cr[*ci] = Value::Null;
                    }
                }
            }
        }

        validate_all_unique_constraints(child_schema, &updated_child_rows)?;
        validate_all_foreign_keys(catalog, storage, child_schema, &updated_child_rows)?;
        let keep_old_indices: Vec<usize> = (0..updated_child_rows.len()).collect();
        storage.replace_rows_with_alignment(&child_table, updated_child_rows, keep_old_indices)?;
        storage.rebuild_indexes(&child_table, child_schema)?;
    }
    Ok(())
}

fn resolve_cols_to_idxs(schema: &Schema, cols: &[String]) -> Result<Vec<usize>, String> {
    cols.iter()
        .map(|c| {
            schema
                .columns
                .iter()
                .position(|x| x.name == *c)
                .ok_or_else(|| format!("Unknown column '{}' in FOREIGN KEY", c))
        })
        .collect()
}

fn tuple_eq(a_row: &Row, a_idxs: &[usize], b_row: &Row, b_idxs: &[usize]) -> bool {
    a_idxs
        .iter()
        .zip(b_idxs.iter())
        .all(|(ai, bi)| a_row.get(*ai) == b_row.get(*bi))
}

fn fk_parent_exists(
    _catalog: &Catalog,
    storage: &dyn StorageEngine,
    parent_table: &str,
    parent_schema: &Schema,
    child_row: &Row,
    child_idxs: &[usize],
    parent_idxs: &[usize],
) -> Result<bool, String> {
    if child_idxs.len() == 1 && parent_idxs.len() == 1 {
        let child_idx = child_idxs[0];
        let parent_idx = parent_idxs[0];
        if let Some(v) = child_row.get(child_idx) {
            let tok = value_to_string(v);
            let parent_col = &parent_schema.columns[parent_idx].name;
            if parent_schema.primary_key.len() == 1
                && parent_schema.primary_key.first().is_some_and(|c| c == parent_col)
                && storage
                    .lookup_pk_row_index(parent_table, parent_schema, &tok)?
                    .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_unique_row_index(parent_table, parent_schema, parent_col, &tok)?
                .is_some()
            {
                return Ok(true);
            }
        }
    }

    let parent_rows = storage.scan(parent_table)?;
    Ok(parent_rows
        .iter()
        .any(|pr| tuple_eq(child_row, child_idxs, pr, parent_idxs)))
}

fn fk_child_references_parent(
    storage: &dyn StorageEngine,
    child_table: &str,
    child_schema: &Schema,
    parent_row: &Row,
    child_idxs: &[usize],
    parent_idxs: &[usize],
) -> Result<bool, String> {
    if child_idxs.len() == 1 && parent_idxs.len() == 1 {
        let child_idx = child_idxs[0];
        let parent_idx = parent_idxs[0];
        if let Some(v) = parent_row.get(parent_idx) {
            let tok = value_to_string(v);
            let child_col = &child_schema.columns[child_idx].name;

            if child_schema.primary_key.len() == 1
                && child_schema.primary_key.first().is_some_and(|c| c == child_col)
                && storage
                    .lookup_pk_row_index(child_table, child_schema, &tok)?
                    .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_unique_row_index(child_table, child_schema, child_col, &tok)?
                .is_some()
            {
                return Ok(true);
            }
            if storage
                .lookup_secondary_row_indices(child_table, child_schema, child_col, &tok)?
                .is_some_and(|hits| !hits.is_empty())
            {
                return Ok(true);
            }
        }
    }

    let child_rows = storage.scan(child_table)?;
    Ok(child_rows
        .iter()
        .any(|cr| tuple_eq(cr, child_idxs, parent_row, parent_idxs)))
}
