impl Catalog {
    pub fn save_to_path(&self, path: &Path) -> Result<(), String> {
        let mut tables: HashMap<String, Vec<ColumnFile>> = HashMap::new();
        let mut table_constraints: HashMap<String, TableConstraintFile> = HashMap::new();
        for (table, schema) in &self.tables {
            let cols: Vec<ColumnFile> = schema
                .columns
                .iter()
                .map(|c| {
                    let dtype = match &c.dtype {
                        DataType::Bool => "bool".to_string(),
                        DataType::Int => "int".to_string(),
                        DataType::BigInt => "bigint".to_string(),
                        DataType::Decimal { precision, scale } => {
                            format!("decimal({precision},{scale})")
                        }
                        DataType::VarChar(n) => format!("varchar({n})"),
                        DataType::Text => "text".to_string(),
                        DataType::Date => "date".to_string(),
                        DataType::Timestamp => "timestamp".to_string(),
                        DataType::Uuid => "uuid".to_string(),
                        DataType::Json => "json".to_string(),
                        DataType::Blob => "blob".to_string(),
                    };
                    ColumnFile {
                        name: c.name.clone(),
                        dtype,
                        primary_key: c.primary_key,
                        unique: c.unique,
                        not_null: c.not_null,
                    }
                })
                .collect();
            tables.insert(table.clone(), cols);
            table_constraints.insert(
                table.clone(),
                TableConstraintFile {
                    primary_key: schema.primary_key.clone(),
                    unique: schema.unique_constraints.clone(),
                    secondary_indexes: schema.secondary_indexes.clone(),
                    foreign_keys: schema
                        .foreign_keys
                        .iter()
                        .map(|fk| ForeignKeyFile {
                            columns: fk.columns.clone(),
                            ref_table: fk.ref_table.clone(),
                            ref_columns: fk.ref_columns.clone(),
                            on_delete: match fk.on_delete {
                                ForeignKeyAction::Restrict => "restrict".to_string(),
                                ForeignKeyAction::Cascade => "cascade".to_string(),
                                ForeignKeyAction::SetNull => "set null".to_string(),
                                ForeignKeyAction::NoAction => "no action".to_string(),
                            },
                            on_update: match fk.on_update {
                                ForeignKeyAction::Restrict => "restrict".to_string(),
                                ForeignKeyAction::Cascade => "cascade".to_string(),
                                ForeignKeyAction::SetNull => "set null".to_string(),
                                ForeignKeyAction::NoAction => "no action".to_string(),
                            },
                        })
                        .collect(),
                },
            );
        }

        let payload = serde_json::to_string_pretty(&CatalogFile { tables, table_constraints })
            .map_err(|e| format!("Failed to serialize catalog as JSON: {e}"))?;
        fs::write(path, payload).map_err(|e| format!("Failed to write catalog file: {e}"))
    }

    /// Loads catalog metadata from disk.
    pub fn load_from_path(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read catalog file: {e}"))?;
        if content.trim().is_empty() {
            return Ok(Self::new());
        }

        let file: CatalogFile = serde_json::from_str(&content)
            .map_err(|e| format!("Malformed catalog JSON: {e}"))?;
        let CatalogFile {
            tables: file_tables,
            table_constraints: file_constraints,
        } = file;
        let mut tables: HashMap<String, Schema> = HashMap::new();
        for (table, cols) in file_tables {
            let mut columns: Vec<Column> = Vec::new();
            for c in cols {
                let dtype = crate::types::datatype::parse_datatype(&c.dtype)?;
                columns.push(Column {
                    name: c.name,
                    dtype,
                    primary_key: c.primary_key,
                    unique: c.unique,
                    not_null: c.not_null,
                });
            }
            let tc = file_constraints.get(&table).cloned().unwrap_or_default();
            tables.insert(
                table.clone(),
                {
                    let mut schema = Schema::with_constraints(
                        columns,
                        tc.primary_key,
                        tc.unique,
                        tc.foreign_keys
                            .into_iter()
                            .map(|fk| ForeignKeyDef {
                                columns: fk.columns,
                                ref_table: fk.ref_table,
                                ref_columns: fk.ref_columns,
                                on_delete: match fk.on_delete.to_lowercase().as_str() {
                                    "cascade" => ForeignKeyAction::Cascade,
                                    "set null" => ForeignKeyAction::SetNull,
                                    "no action" => ForeignKeyAction::NoAction,
                                    _ => ForeignKeyAction::Restrict,
                                },
                                on_update: match fk.on_update.to_lowercase().as_str() {
                                    "cascade" => ForeignKeyAction::Cascade,
                                    "set null" => ForeignKeyAction::SetNull,
                                    "no action" => ForeignKeyAction::NoAction,
                                    _ => ForeignKeyAction::Restrict,
                                },
                            })
                            .collect(),
                    );
                    schema.secondary_indexes = tc.secondary_indexes;
                    schema
                },
            );
        }

        Ok(Self { tables })
    }
}
