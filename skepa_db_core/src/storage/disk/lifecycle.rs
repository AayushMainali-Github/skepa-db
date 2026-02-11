impl DiskStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        initialize_layout(&root)?;
        Ok(Self {
            root,
            tables: HashMap::new(),
            row_ids: HashMap::new(),
            next_row_id: HashMap::new(),
            pk_indexes: HashMap::new(),
            unique_indexes: HashMap::new(),
            secondary_indexes: HashMap::new(),
        })
    }

    fn table_file_path(&self, table: &str) -> PathBuf {
        self.root.join("tables").join(format!("{table}.rows"))
    }

    fn index_file_path(&self, table: &str) -> PathBuf {
        self.root.join("indexes").join(format!("{table}.indexes.json"))
    }

    pub fn bootstrap_table(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        if self.tables.contains_key(table) {
            return Ok(());
        }
        let table_file = self.table_file_path(table);
        if !table_file.exists() {
            File::create(&table_file)
                .map_err(|e| format!("Failed to create table file for '{table}': {e}"))?;
        }

        let content = fs::read_to_string(&table_file)
            .map_err(|e| format!("Failed to read table file for '{table}': {e}"))?;
        let mut rows: Vec<Row> = Vec::new();
        let mut row_ids: Vec<u64> = Vec::new();
        let mut max_row_id = 0u64;

        for (line_no, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let mut tokens: Vec<&str> = line.split('\t').collect();
            let parsed_row_id = parse_row_id_prefix(tokens.first().copied().unwrap_or(""));
            let row_id = if let Some(id) = parsed_row_id {
                tokens.remove(0);
                id
            } else {
                (line_no as u64) + 1
            };
            if tokens.len() != schema.columns.len() {
                return Err(format!(
                    "Malformed row in table '{}' at line {}: expected {} values, got {}",
                    table,
                    line_no + 1,
                    schema.columns.len(),
                    tokens.len()
                ));
            }

            let mut row: Row = Vec::with_capacity(tokens.len());
            for (i, tok) in tokens.iter().enumerate() {
                let dtype = &schema.columns[i].dtype;
                let decoded = decode_token(tok, dtype)?;
                row.push(parse_value(dtype, &decoded)?);
            }
            rows.push(row);
            row_ids.push(row_id);
            if row_id > max_row_id {
                max_row_id = row_id;
            }
        }

        self.tables.insert(table.to_string(), rows);
        self.row_ids.insert(table.to_string(), row_ids);
        self.next_row_id.insert(table.to_string(), max_row_id + 1);
        if self.load_indexes_from_disk(table, schema).is_err() {
            self.rebuild_indexes_internal(table, schema)?;
            self.persist_indexes(table)?;
        }
        Ok(())
    }

    pub fn checkpoint_all(&self) -> Result<(), String> {
        let mut names: Vec<&String> = self.tables.keys().collect();
        names.sort();
        for table in names {
            self.persist_table(table)?;
        }
        Ok(())
    }

    pub fn persist_table(&self, table: &str) -> Result<(), String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let row_ids = self
            .row_ids
            .get(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        if rows.len() != row_ids.len() {
            return Err(format!("Table '{}' row-id alignment is corrupted", table));
        }
        let table_file = self.table_file_path(table);
        let mut lines: Vec<String> = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let encoded = row
                .iter()
                .map(encode_value)
                .collect::<Vec<_>>()
                .join("\t");
            lines.push(format!("@{}|\t{}", row_ids[i], encoded));
        }
        let payload = if lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", lines.join("\n"))
        };
        fs::write(table_file, payload)
            .map_err(|e| format!("Failed to write table snapshot for '{table}': {e}"))?;
        self.persist_indexes(table)
    }
}

fn initialize_layout(root: &Path) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|e| format!("Failed to create db directory: {e}"))?;
    fs::create_dir_all(root.join("tables"))
        .map_err(|e| format!("Failed to create tables directory: {e}"))?;
    fs::create_dir_all(root.join("indexes"))
        .map_err(|e| format!("Failed to create indexes directory: {e}"))?;

    let catalog = root.join("catalog.json");
    if !catalog.exists() {
        File::create(&catalog).map_err(|e| format!("Failed to create catalog file: {e}"))?;
    }

    let wal = root.join("wal.log");
    if !wal.exists() {
        File::create(&wal).map_err(|e| format!("Failed to create WAL file: {e}"))?;
    }

    Ok(())
}
