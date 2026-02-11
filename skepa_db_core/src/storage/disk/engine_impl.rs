impl StorageEngine for DiskStorage {
    fn create_table(&mut self, table: &str) -> Result<(), String> {
        if self.tables.contains_key(table) {
            return Err(format!("Table '{}' already exists in storage", table));
        }

        let table_file = self.table_file_path(table);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(table_file)
            .map_err(|e| format!("Failed to create table file for '{table}': {e}"))?;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.index_file_path(table))
            .map_err(|e| format!("Failed to create index file for '{table}': {e}"))?;
        self.tables.insert(table.to_string(), Vec::new());
        self.row_ids.insert(table.to_string(), Vec::new());
        self.next_row_id.insert(table.to_string(), 1);
        self.pk_indexes.remove(table);
        self.unique_indexes.remove(table);
        self.secondary_indexes.remove(table);
        Ok(())
    }

    fn insert_row(&mut self, table: &str, row: Row) -> Result<(), String> {
        let rows = self
            .tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let ids = self
            .row_ids
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        let next = self
            .next_row_id
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' next row id is missing", table))?;
        rows.push(row);
        ids.push(*next);
        *next += 1;
        Ok(())
    }

    fn scan(&self, table: &str) -> Result<&[Row], String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        Ok(rows.as_slice())
    }

    fn scan_mut(&mut self, table: &str) -> Result<&mut Vec<Row>, String> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))
    }

    fn replace_rows_with_alignment(
        &mut self,
        table: &str,
        new_rows: Vec<Row>,
        old_indices: Vec<usize>,
    ) -> Result<(), String> {
        if new_rows.len() != old_indices.len() {
            return Err("Row replacement alignment mismatch".to_string());
        }
        let old_ids = self
            .row_ids
            .get(table)
            .cloned()
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        let mut new_ids: Vec<u64> = Vec::with_capacity(new_rows.len());
        for old_i in old_indices {
            let id = old_ids
                .get(old_i)
                .copied()
                .ok_or_else(|| "Row replacement old index out of range".to_string())?;
            new_ids.push(id);
        }
        self.tables.insert(table.to_string(), new_rows);
        self.row_ids.insert(table.to_string(), new_ids);
        Ok(())
    }

    fn lookup_pk_row_index(
        &self,
        table: &str,
        schema: &Schema,
        rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        if schema.primary_key.len() != 1 {
            return Ok(None);
        }
        let pk_col = &schema.primary_key[0];
        let col_idx = schema
            .columns
            .iter()
            .position(|c| c.name == *pk_col)
            .ok_or_else(|| format!("Unknown column '{}' in primary key", pk_col))?;
        let dtype = &schema.columns[col_idx].dtype;
        let rhs = parse_value(dtype, rhs_token)?;
        let key = encode_key_parts(&[value_to_string(&rhs)]);
        let row_id = self
            .pk_indexes
            .get(table)
            .and_then(|idx| if idx.col_idxs.as_slice() == [col_idx] { idx.map.get(&key).copied() } else { None });
        Ok(row_id.and_then(|rid| self.row_index_by_id(table, rid)))
    }

    fn rebuild_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        self.rebuild_indexes_internal(table, schema)
    }

    fn lookup_pk_conflict(
        &self,
        table: &str,
        schema: &Schema,
        candidate: &Row,
        skip_idx: Option<usize>,
    ) -> Result<Option<usize>, String> {
        if schema.primary_key.is_empty() {
            return Ok(None);
        }
        let idx = match self.pk_indexes.get(table) {
            Some(i) => i,
            None => return Ok(None),
        };
        let parts = idx
            .col_idxs
            .iter()
            .map(|i| {
                candidate
                    .get(*i)
                    .map(value_to_string)
                    .ok_or_else(|| "Candidate row missing PK column".to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = encode_key_parts(&parts);
        let hit = idx.map.get(&key).copied();
        let skip_row_id = skip_idx
            .and_then(|i| self.row_ids.get(table).and_then(|ids| ids.get(i).copied()));
        Ok(match (hit, skip_idx) {
            (Some(found), Some(_)) if skip_row_id == Some(found) => None,
            (Some(found), _) => self.row_index_by_id(table, found),
            (None, _) => None,
        })
    }

    fn lookup_unique_row_index(
        &self,
        table: &str,
        schema: &Schema,
        column: &str,
        rhs_token: &str,
    ) -> Result<Option<usize>, String> {
        let indexes = match self.unique_indexes.get(table) {
            Some(v) => v,
            None => return Ok(None),
        };
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == column) else {
            return Ok(None);
        };
        let idx = indexes
            .iter()
            .find(|u| u.col_idxs.len() == 1 && u.col_idxs[0] == col_idx);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let dtype = &schema.columns[col_idx].dtype;
        let rhs = parse_value(dtype, rhs_token)?;
        let key = encode_key_parts(&[value_to_string(&rhs)]);
        let row_id = idx.map.get(&key).copied();
        Ok(row_id.and_then(|rid| self.row_index_by_id(table, rid)))
    }

    fn lookup_unique_conflict(
        &self,
        table: &str,
        _schema: &Schema,
        candidate: &Row,
        skip_idx: Option<usize>,
    ) -> Result<Option<Vec<String>>, String> {
        let indexes = match self.unique_indexes.get(table) {
            Some(v) => v,
            None => return Ok(None),
        };
        for idx in indexes {
            let has_null = idx
                .col_idxs
                .iter()
                .any(|i| matches!(candidate.get(*i), Some(Value::Null)));
            if has_null {
                continue;
            }
            let parts = idx
                .col_idxs
                .iter()
                .map(|i| {
                    candidate
                        .get(*i)
                        .map(value_to_string)
                        .ok_or_else(|| "Candidate row missing UNIQUE column".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let key = encode_key_parts(&parts);
            if let Some(found) = idx.map.get(&key).copied() {
                let skip_row_id = skip_idx
                    .and_then(|i| self.row_ids.get(table).and_then(|ids| ids.get(i).copied()));
                if skip_row_id != Some(found) {
                    return Ok(Some(idx.cols.clone()));
                }
            }
        }
        Ok(None)
    }

    fn lookup_secondary_row_indices(
        &self,
        table: &str,
        schema: &Schema,
        column: &str,
        rhs_token: &str,
    ) -> Result<Option<Vec<usize>>, String> {
        let indexes = match self.secondary_indexes.get(table) {
            Some(v) => v,
            None => return Ok(None),
        };
        let Some(col_idx) = schema.columns.iter().position(|c| c.name == column) else {
            return Ok(None);
        };
        let idx = indexes
            .iter()
            .find(|s| s.col_idxs.len() == 1 && s.col_idxs[0] == col_idx);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let dtype = &schema.columns[col_idx].dtype;
        let rhs = parse_value(dtype, rhs_token)?;
        let key = encode_key_parts(&[value_to_string(&rhs)]);
        let row_ids = match idx.map.get(&key) {
            Some(v) => v,
            None => return Ok(Some(Vec::new())),
        };
        let rows = row_ids
            .iter()
            .filter_map(|rid| self.row_index_by_id(table, *rid))
            .collect::<Vec<_>>();
        Ok(Some(rows))
    }
}

impl DiskStorage {
    fn row_index_by_id(&self, table: &str, row_id: u64) -> Option<usize> {
        self.row_ids
            .get(table)
            .and_then(|ids| ids.iter().position(|id| *id == row_id))
    }

    fn persist_indexes(&self, table: &str) -> Result<(), String> {
        let pk = self.pk_indexes.get(table).map(|idx| IndexSnapshot {
            cols: Vec::new(),
            col_idxs: idx.col_idxs.clone(),
            entries: idx
                .map
                .iter()
                .map(|(k, v)| IndexEntry {
                    key: k.clone(),
                    row_id: *v,
                })
                .collect(),
        });

        let unique = self
            .unique_indexes
            .get(table)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|u| IndexSnapshot {
                cols: u.cols,
                col_idxs: u.col_idxs,
                entries: u
                    .map
                    .into_iter()
                    .map(|(k, v)| IndexEntry { key: k, row_id: v })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let secondary = self
            .secondary_indexes
            .get(table)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|s| SecondaryIndexSnapshot {
                cols: s.cols,
                col_idxs: s.col_idxs,
                entries: s
                    .map
                    .into_iter()
                    .map(|(k, v)| SecondaryIndexEntry { key: k, row_ids: v })
                    .collect(),
            })
            .collect::<Vec<_>>();

        let payload = serde_json::to_string_pretty(&TableIndexSnapshot { pk, unique, secondary })
            .map_err(|e| format!("Failed to serialize indexes for '{table}': {e}"))?;
        fs::write(self.index_file_path(table), payload)
            .map_err(|e| format!("Failed to write index file for '{table}': {e}"))
    }

    fn load_indexes_from_disk(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        let path = self.index_file_path(table);
        if !path.exists() {
            return Err("Index file missing".to_string());
        }
        let content = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read index file for '{table}': {e}"))?;
        if content.trim().is_empty() {
            return Err("Index file is empty".to_string());
        }
        let snapshot: TableIndexSnapshot = serde_json::from_str(&content)
            .map_err(|e| format!("Malformed index file for '{table}': {e}"))?;

        self.rebuild_indexes_internal(table, schema)?;

        let row_ids = self
            .row_ids
            .get(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing in storage", table))?
            .clone();

        let mut should_heal = false;

        if let (Some(idx), Some(snap)) = (self.pk_indexes.get_mut(table), snapshot.pk) {
            if idx.col_idxs == snap.col_idxs {
                match validate_snapshot_entries(snap.entries, &row_ids) {
                    Ok(map) => idx.map = map,
                    Err(_) => should_heal = true,
                }
            } else {
                should_heal = true;
            }
        }

        if let Some(existing) = self.unique_indexes.get_mut(table) {
            for u in existing {
                if let Some(su) = snapshot
                    .unique
                    .iter()
                    .find(|s| s.col_idxs == u.col_idxs && s.cols == u.cols)
                {
                    match validate_snapshot_entries(su.entries.clone(), &row_ids) {
                        Ok(map) => u.map = map,
                        Err(_) => should_heal = true,
                    }
                } else {
                    should_heal = true;
                }
            }
        }
        if let Some(existing) = self.secondary_indexes.get_mut(table) {
            for s in existing {
                if let Some(ss) = snapshot
                    .secondary
                    .iter()
                    .find(|x| x.col_idxs == s.col_idxs && x.cols == s.cols)
                {
                    match validate_secondary_snapshot_entries(ss.entries.clone(), &row_ids) {
                        Ok(map) => s.map = map,
                        Err(_) => should_heal = true,
                    }
                } else {
                    should_heal = true;
                }
            }
        }
        if should_heal {
            self.persist_indexes(table)?;
        }
        Ok(())
    }

    fn rebuild_indexes_internal(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        self.rebuild_primary_index(table, schema)?;
        self.rebuild_unique_indexes(table, schema)?;
        self.rebuild_secondary_indexes(table, schema)
    }

    fn rebuild_primary_index(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        if schema.primary_key.is_empty() {
            self.pk_indexes.remove(table);
            return Ok(());
        }
        let mut col_idxs: Vec<usize> = Vec::new();
        for pk_col in &schema.primary_key {
            let col_idx = schema
                .columns
                .iter()
                .position(|c| c.name == *pk_col)
                .ok_or_else(|| format!("Unknown column '{}' in primary key", pk_col))?;
            col_idxs.push(col_idx);
        }
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let ids = self
            .row_ids
            .get(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        let mut map: BTreeMap<String, u64> = BTreeMap::new();
        for (row_idx, row) in rows.iter().enumerate() {
            let mut parts: Vec<String> = Vec::new();
            for (i, pk_col) in col_idxs.iter().zip(schema.primary_key.iter()) {
                let v = row
                    .get(*i)
                    .ok_or_else(|| format!("Row is missing PK column '{}'", pk_col))?;
                parts.push(value_to_string(v));
            }
            let row_id = *ids
                .get(row_idx)
                .ok_or_else(|| format!("Table '{}' row-id alignment is corrupted", table))?;
            map.insert(encode_key_parts(&parts), row_id);
        }
        self.pk_indexes
            .insert(table.to_string(), PrimaryIndex { col_idxs, map });
        Ok(())
    }

    fn rebuild_unique_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let ids = self
            .row_ids
            .get(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        let groups = unique_groups(schema)?;
        if groups.is_empty() {
            self.unique_indexes.remove(table);
            return Ok(());
        }
        let mut indexes: Vec<UniqueIndex> = Vec::new();
        for cols in groups {
            let mut col_idxs = Vec::new();
            for c in &cols {
                let i = schema
                    .columns
                    .iter()
                    .position(|x| x.name == *c)
                    .ok_or_else(|| format!("Unknown UNIQUE column '{}'", c))?;
                col_idxs.push(i);
            }
            let mut map: BTreeMap<String, u64> = BTreeMap::new();
            for (row_idx, row) in rows.iter().enumerate() {
                if col_idxs
                    .iter()
                    .any(|i| matches!(row.get(*i), Some(Value::Null)))
                {
                    continue;
                }
                let parts = col_idxs
                    .iter()
                    .map(|i| {
                        row.get(*i)
                            .map(value_to_string)
                            .ok_or_else(|| "Row missing UNIQUE column".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_id = *ids
                    .get(row_idx)
                    .ok_or_else(|| format!("Table '{}' row-id alignment is corrupted", table))?;
                map.insert(encode_key_parts(&parts), row_id);
            }
            indexes.push(UniqueIndex { cols, col_idxs, map });
        }
        self.unique_indexes.insert(table.to_string(), indexes);
        Ok(())
    }

    fn rebuild_secondary_indexes(&mut self, table: &str, schema: &Schema) -> Result<(), String> {
        let rows = self
            .tables
            .get(table)
            .ok_or_else(|| format!("Table '{}' does not exist in storage", table))?;
        let ids = self
            .row_ids
            .get(table)
            .ok_or_else(|| format!("Table '{}' row ids are missing", table))?;
        if schema.secondary_indexes.is_empty() {
            self.secondary_indexes.remove(table);
            return Ok(());
        }
        let mut indexes: Vec<SecondaryIndex> = Vec::new();
        for cols in &schema.secondary_indexes {
            let mut col_idxs = Vec::new();
            for c in cols {
                let i = schema
                    .columns
                    .iter()
                    .position(|x| x.name == *c)
                    .ok_or_else(|| format!("Unknown INDEX column '{}'", c))?;
                col_idxs.push(i);
            }
            let mut map: BTreeMap<String, Vec<u64>> = BTreeMap::new();
            for (row_idx, row) in rows.iter().enumerate() {
                if col_idxs
                    .iter()
                    .any(|i| matches!(row.get(*i), Some(Value::Null)))
                {
                    continue;
                }
                let parts = col_idxs
                    .iter()
                    .map(|i| {
                        row.get(*i)
                            .map(value_to_string)
                            .ok_or_else(|| "Row missing INDEX column".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let key = encode_key_parts(&parts);
                let row_id = *ids
                    .get(row_idx)
                    .ok_or_else(|| format!("Table '{}' row-id alignment is corrupted", table))?;
                map.entry(key).or_default().push(row_id);
            }
            indexes.push(SecondaryIndex {
                cols: cols.clone(),
                col_idxs,
                map,
            });
        }
        self.secondary_indexes.insert(table.to_string(), indexes);
        Ok(())
    }
}
