impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Checks if a table exists in the catalog
    pub fn exists(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Creates a new table schema in the catalog
    /// Returns an error if the table already exists
    pub fn create_table(
        &mut self,
        table: String,
        cols: Vec<ColumnDef>,
        table_constraints: Vec<TableConstraintDef>,
    ) -> Result<(), String> {
        if self.exists(&table) {
            return Err(format!("Table '{}' already exists", table));
        }

        let mut primary_key: Vec<String> = Vec::new();
        let mut unique_constraints: Vec<Vec<String>> = Vec::new();
        let mut foreign_keys: Vec<ForeignKeyDef> = Vec::new();

        let columns: Vec<Column> = cols
            .into_iter()
            .map(|c| Column {
                name: c.name,
                dtype: c.dtype,
                primary_key: c.primary_key,
                unique: c.unique,
                not_null: c.not_null,
            })
            .collect();

        for c in &columns {
            if c.primary_key {
                primary_key.push(c.name.clone());
            }
            if c.unique && !c.primary_key {
                unique_constraints.push(vec![c.name.clone()]);
            }
        }

        if primary_key.len() > 1 {
            return Err("Only one PRIMARY KEY constraint is supported".to_string());
        }

        for tc in table_constraints {
            match tc {
                TableConstraintDef::PrimaryKey(cols) => {
                    if !primary_key.is_empty() {
                        return Err("Only one PRIMARY KEY constraint is supported".to_string());
                    }
                    primary_key = cols;
                }
                TableConstraintDef::Unique(cols) => {
                    unique_constraints.push(cols);
                }
                TableConstraintDef::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                } => {
                    foreign_keys.push(ForeignKeyDef {
                        columns,
                        ref_table,
                        ref_columns,
                        on_delete,
                        on_update,
                    });
                }
            }
        }

        if primary_key.is_empty() {
            // keep empty
        } else {
            for pk_col in &primary_key {
                if let Some(col) = columns.iter().find(|c| &c.name == pk_col) {
                    if !col.not_null {
                        // enforced semantically by PK
                    }
                } else {
                    return Err(format!("PRIMARY KEY references unknown column '{pk_col}'"));
                }
            }
        }

        for uniq in &unique_constraints {
            for c in uniq {
                if columns.iter().all(|col| &col.name != c) {
                    return Err(format!("UNIQUE references unknown column '{c}'"));
                }
            }
        }

        for fk in &foreign_keys {
            if fk.columns.is_empty() || fk.ref_columns.is_empty() {
                return Err("FOREIGN KEY column list cannot be empty".to_string());
            }
            if fk.columns.len() != fk.ref_columns.len() {
                return Err("FOREIGN KEY column count must match referenced column count".to_string());
            }
            for c in &fk.columns {
                if columns.iter().all(|col| &col.name != c) {
                    return Err(format!("FOREIGN KEY references unknown column '{c}'"));
                }
            }
            if matches!(fk.on_delete, ForeignKeyAction::SetNull)
                || matches!(fk.on_update, ForeignKeyAction::SetNull)
            {
                for c in &fk.columns {
                    let child_col = columns
                        .iter()
                        .find(|col| &col.name == c)
                        .ok_or_else(|| format!("FOREIGN KEY references unknown column '{c}'"))?;
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
                if parent.columns.iter().all(|col| &col.name != c) {
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
        }

        let mut schema = Schema::with_constraints(
            columns,
            primary_key.clone(),
            unique_constraints.clone(),
            foreign_keys.clone(),
        );
        // PK implies NOT NULL on referenced columns.
        for c in &mut schema.columns {
            if primary_key.iter().any(|pk| pk == &c.name) {
                c.not_null = true;
            }
        }
        self.tables.insert(table, schema);
        Ok(())
    }

    /// Retrieves the schema for a given table
    /// Returns an error if the table does not exist
    pub fn schema(&self, table: &str) -> Result<&Schema, String> {
        self.tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))
    }
}
