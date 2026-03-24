use super::*;

impl Database {
    pub(super) fn alloc_txid(&mut self) -> u64 {
        let txid = self.next_txid;
        self.next_txid += 1;
        txid
    }

    pub(super) fn handle_begin(&mut self) -> Result<String, String> {
        if self.current_tx.is_some() {
            return Err("Transaction already active".to_string());
        }
        let mut table_versions_at_begin: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        for (table, _) in self.catalog.snapshot_tables() {
            let ver = self.table_file_version(&table)?;
            table_versions_at_begin.insert(table, ver);
        }
        let tx = TxState {
            txid: self.alloc_txid(),
            staged_ops: Vec::new(),
            touched_tables: std::collections::HashSet::new(),
            table_versions_at_begin,
            snapshot_catalog: self.catalog.clone(),
            snapshot_storage: self.storage.clone(),
        };
        self.current_tx = Some(tx);
        Ok("transaction started".to_string())
    }

    pub(super) fn handle_commit(&mut self) -> Result<String, String> {
        let snapshot_catalog = self
            .current_tx
            .as_ref()
            .ok_or_else(|| "No active transaction".to_string())?
            .snapshot_catalog
            .clone();
        let snapshot_storage = self
            .current_tx
            .as_ref()
            .ok_or_else(|| "No active transaction".to_string())?
            .snapshot_storage
            .clone();

        if let Err(e) = engine::validate_no_action_constraints(&self.catalog, &self.storage) {
            self.catalog = snapshot_catalog;
            self.storage = snapshot_storage;
            self.current_tx = None;
            return Err(e);
        }

        let tx = self
            .current_tx
            .take()
            .ok_or_else(|| "No active transaction".to_string())?;

        for table in &tx.touched_tables {
            let begin_ver = tx.table_versions_at_begin.get(table).copied().unwrap_or(0);
            let now_ver = self.table_file_version(table)?;
            if now_ver != begin_ver {
                self.reload_from_disk()?;
                return Err(format!(
                    "Transaction conflict on table '{}': data changed outside this transaction",
                    table
                ));
            }
        }

        if !tx.staged_ops.is_empty() {
            self.append_wal_line(&format!("BEGIN {}", tx.txid))?;
            for op in &tx.staged_ops {
                self.append_wal_line(&format!("OP {} {}", tx.txid, op))?;
            }
            self.append_wal_line(&format!("COMMIT {}", tx.txid))?;

            for table in &tx.touched_tables {
                self.storage.persist_table(table)?;
            }
            self.checkpoint_and_truncate_wal()?;
        }
        Ok("transaction committed".to_string())
    }

    pub(super) fn handle_rollback(&mut self) -> Result<String, String> {
        let tx = self
            .current_tx
            .take()
            .ok_or_else(|| "No active transaction".to_string())?;
        self.catalog = tx.snapshot_catalog;
        self.storage = tx.snapshot_storage;
        Ok("transaction rolled back".to_string())
    }

    pub(super) fn table_file_version(&self, table: &str) -> Result<u64, String> {
        let path = self.path.join("tables").join(format!("{table}.rows"));
        let bytes = fs::read(&path).map_err(|e| {
            format!(
                "Failed to read table file for '{table}' while checking transaction conflict: {e}"
            )
        })?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        Ok(h.finish())
    }

    pub(super) fn reload_from_disk(&mut self) -> Result<(), String> {
        let catalog_path = self.path.join("catalog.json");
        let mut storage = DiskStorage::new(self.path.clone())?;
        let catalog = Catalog::load_from_path(&catalog_path).unwrap_or_else(|_| Catalog::new());
        for (table, _) in catalog.snapshot_tables() {
            let schema = catalog
                .schema(&table)
                .map_err(|e| format!("Failed to refresh schema for '{table}': {e}"))?;
            storage
                .bootstrap_table(&table, schema)
                .map_err(|e| format!("Failed to refresh table '{table}' from disk: {e}"))?;
        }
        self.catalog = catalog;
        self.storage = storage;
        self.current_tx = None;
        Ok(())
    }
}
