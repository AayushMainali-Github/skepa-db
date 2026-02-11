impl Catalog {
    pub fn add_unique_constraint(&mut self, table: &str, mut cols: Vec<String>) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        if cols.is_empty() {
            return Err("UNIQUE column list cannot be empty".to_string());
        }
        for c in &cols {
            if schema.columns.iter().all(|col| col.name != *c) {
                return Err(format!("UNIQUE references unknown column '{}'", c));
            }
        }
        if schema.unique_constraints.iter().any(|u| u == &cols) {
            return Err(format!(
                "UNIQUE constraint on ({}) already exists",
                cols.join(",")
            ));
        }
        cols.shrink_to_fit();
        schema.unique_constraints.push(cols);
        Ok(())
    }

    pub fn add_secondary_index(&mut self, table: &str, cols: Vec<String>) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        if cols.is_empty() {
            return Err("INDEX column list cannot be empty".to_string());
        }
        for c in &cols {
            if schema.columns.iter().all(|col| col.name != *c) {
                return Err(format!("INDEX references unknown column '{}'", c));
            }
        }
        if schema.secondary_indexes.iter().any(|x| x == &cols) {
            return Err(format!("INDEX on ({}) already exists", cols.join(",")));
        }
        schema.secondary_indexes.push(cols);
        Ok(())
    }

    pub fn drop_secondary_index(&mut self, table: &str, cols: &[String]) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        let before = schema.secondary_indexes.len();
        schema.secondary_indexes.retain(|x| x != cols);
        if before == schema.secondary_indexes.len() {
            return Err(format!("INDEX on ({}) does not exist", cols.join(",")));
        }
        Ok(())
    }

    pub fn drop_unique_constraint(&mut self, table: &str, cols: &[String]) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        let before = schema.unique_constraints.len();
        schema.unique_constraints.retain(|u| u != cols);
        if schema.unique_constraints.len() == before {
            return Err(format!(
                "UNIQUE constraint on ({}) does not exist",
                cols.join(",")
            ));
        }
        Ok(())
    }

    pub fn add_foreign_key_constraint(
        &mut self,
        table: &str,
        fk: ForeignKeyDef,
    ) -> Result<(), String> {
        // Read-only validation first.
        let child_schema = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        if fk.columns.is_empty() || fk.ref_columns.is_empty() {
            return Err("FOREIGN KEY column list cannot be empty".to_string());
        }
        if fk.columns.len() != fk.ref_columns.len() {
            return Err("FOREIGN KEY column count must match referenced column count".to_string());
        }
        for c in &fk.columns {
            if child_schema.columns.iter().all(|col| col.name != *c) {
                return Err(format!("FOREIGN KEY references unknown column '{}'", c));
            }
        }
        if matches!(fk.on_delete, ForeignKeyAction::SetNull)
            || matches!(fk.on_update, ForeignKeyAction::SetNull)
        {
            for c in &fk.columns {
                let child_col = child_schema
                    .columns
                    .iter()
                    .find(|col| col.name == *c)
                    .ok_or_else(|| format!("FOREIGN KEY references unknown column '{}'", c))?;
                if child_col.not_null {
                    return Err(format!(
                        "FOREIGN KEY SET NULL requires nullable child column '{}'",
                        c
                    ));
                }
            }
        }
        let parent = self
            .tables
            .get(&fk.ref_table)
            .ok_or_else(|| format!("FOREIGN KEY references unknown table '{}'", fk.ref_table))?;
        for c in &fk.ref_columns {
            if parent.columns.iter().all(|col| col.name != *c) {
                return Err(format!(
                    "FOREIGN KEY references unknown parent column '{}.{}'",
                    fk.ref_table, c
                ));
            }
        }
        let ref_is_pk = parent.primary_key == fk.ref_columns;
        let ref_is_unique = parent
            .unique_constraints
            .iter()
            .any(|u| u == &fk.ref_columns);
        if !(ref_is_pk || ref_is_unique) {
            return Err(format!(
                "FOREIGN KEY reference {}({}) must target PRIMARY KEY or UNIQUE columns",
                fk.ref_table,
                fk.ref_columns.join(",")
            ));
        }

        let child_schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        if child_schema.foreign_keys.iter().any(|x| {
            x.columns == fk.columns
                && x.ref_table == fk.ref_table
                && x.ref_columns == fk.ref_columns
        }) {
            return Err("FOREIGN KEY constraint already exists".to_string());
        }
        child_schema.foreign_keys.push(fk);
        Ok(())
    }

    pub fn drop_foreign_key_constraint(
        &mut self,
        table: &str,
        columns: &[String],
        ref_table: &str,
        ref_columns: &[String],
    ) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        let before = schema.foreign_keys.len();
        schema.foreign_keys.retain(|fk| {
            !(fk.columns == columns && fk.ref_table == ref_table && fk.ref_columns == ref_columns)
        });
        if schema.foreign_keys.len() == before {
            return Err("FOREIGN KEY constraint does not exist".to_string());
        }
        Ok(())
    }

    pub fn set_not_null(&mut self, table: &str, column: &str, not_null: bool) -> Result<(), String> {
        let schema = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| format!("Unknown column '{}'", column))?;
        if schema.columns[idx].primary_key && !not_null {
            return Err(format!("Cannot drop NOT NULL from PRIMARY KEY column '{}'", column));
        }
        schema.columns[idx].not_null = not_null;
        Ok(())
    }

    /// Returns cloned table names and schemas for bootstrapping storage.
    pub fn snapshot_tables(&self) -> Vec<(String, Schema)> {
        self.tables
            .iter()
            .map(|(name, schema)| (name.clone(), schema.clone()))
            .collect()
    }

}
