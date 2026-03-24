use super::*;

impl Database {
    pub(super) fn initialize_storage(path: &Path) -> DbResult<DiskStorage> {
        DiskStorage::new(path.to_path_buf()).map_err(DbError::from)
    }

    pub(super) fn load_catalog(path: &Path) -> Catalog {
        let catalog_path = path.join("catalog.json");
        Catalog::load_from_path(&catalog_path).unwrap_or_else(|_| Catalog::new())
    }

    pub(super) fn bootstrap_tables(&mut self) -> DbResult<()> {
        for (table, _) in self.catalog.snapshot_tables() {
            let schema = self.catalog.schema(&table).map_err(DbError::from)?;
            self.storage
                .bootstrap_table(&table, schema)
                .map_err(DbError::from)?;
        }
        Ok(())
    }

    pub(super) fn recover(&mut self) -> DbResult<()> {
        self.replay_wal().map_err(DbError::from)?;
        self.checkpoint_and_truncate_wal().map_err(DbError::from)?;
        Ok(())
    }

    pub(super) fn save_catalog(&self) -> Result<(), String> {
        self.catalog.save_to_path(&self.path.join("catalog.json"))
    }

    pub(super) fn append_wal_line(&self, line: &str) -> Result<(), String> {
        let wal_path = self.path.join("wal.log");
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(|e| format!("Failed to open WAL: {e}"))?;
        f.write_all(line.trim().as_bytes())
            .map_err(|e| format!("Failed to write WAL entry: {e}"))?;
        f.write_all(b"\n")
            .map_err(|e| format!("Failed to write WAL newline: {e}"))?;
        Ok(())
    }

    pub(super) fn replay_wal(&mut self) -> Result<(), String> {
        let wal_path = self.path.join("wal.log");
        if !wal_path.exists() {
            return Ok(());
        }
        let content =
            fs::read_to_string(&wal_path).map_err(|e| format!("Failed to read WAL: {e}"))?;

        #[derive(Default)]
        struct ReplayTx {
            first_line: usize,
            committed: bool,
            rolled_back: bool,
            ops: Vec<(usize, String)>,
        }

        let mut txs: std::collections::HashMap<u64, ReplayTx> = std::collections::HashMap::new();

        for (idx, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            match parts.first().copied() {
                Some("BEGIN") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed BEGIN record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                }
                Some("OP") => {
                    if parts.len() != 3 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed OP record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.ops.push((idx + 1, parts[2].to_string()));
                }
                Some("COMMIT") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed COMMIT record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.committed = true;
                }
                Some("ROLLBACK") => {
                    if parts.len() != 2 {
                        return Err(format!(
                            "WAL parse error at line {}: malformed ROLLBACK record",
                            idx + 1
                        ));
                    }
                    let txid: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("WAL parse error at line {}: bad txid", idx + 1))?;
                    let tx = txs.entry(txid).or_default();
                    if tx.first_line == 0 {
                        tx.first_line = idx + 1;
                    }
                    tx.rolled_back = true;
                }
                Some(other) => {
                    return Err(format!(
                        "WAL parse error at line {}: unknown record kind '{other}'",
                        idx + 1
                    ));
                }
                None => {}
            }
        }

        let mut ordered_txs: Vec<(usize, ReplayTx)> = txs
            .into_values()
            .filter(|tx| tx.committed && !tx.rolled_back)
            .map(|tx| (tx.first_line, tx))
            .collect();
        ordered_txs.sort_by_key(|(line, _)| *line);

        for (_, tx) in ordered_txs {
            let before_catalog = self.catalog.clone();
            let before_storage = self.storage.clone();
            let mut invalid_tx = false;

            let mut ops = tx.ops;
            ops.sort_by_key(|(line_no, _)| *line_no);

            for (line_no, stmt) in ops {
                let cmd = parser::parser::parse(&stmt)
                    .map_err(|e| format!("WAL parse error at line {}: {}", line_no, e))?;
                if matches!(
                    cmd,
                    Command::Create { .. } | Command::Begin | Command::Commit | Command::Rollback
                ) {
                    continue;
                }
                if let Err(_e) = engine::execute_command(cmd, &mut self.catalog, &mut self.storage)
                {
                    invalid_tx = true;
                    break;
                }
            }

            if invalid_tx
                || engine::validate_no_action_constraints(&self.catalog, &self.storage).is_err()
            {
                self.catalog = before_catalog;
                self.storage = before_storage;
            }
        }

        Ok(())
    }

    pub(super) fn truncate_wal(&self) -> Result<(), String> {
        fs::write(self.path.join("wal.log"), "").map_err(|e| format!("Failed to truncate WAL: {e}"))
    }

    pub(super) fn checkpoint_and_truncate_wal(&self) -> Result<(), String> {
        self.storage.checkpoint_all()?;
        self.truncate_wal()
    }
}
