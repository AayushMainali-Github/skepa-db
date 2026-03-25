use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn write_file_atomic(path: &Path, payload: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("Cannot determine parent directory for '{}'", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| {
        format!(
            "Failed to create parent directory '{}': {e}",
            parent.display()
        )
    })?;

    let temp_path = temp_path_for(path);
    let mut file = File::create(&temp_path)
        .map_err(|e| format!("Failed to create temp file '{}': {e}", temp_path.display()))?;
    file.write_all(payload)
        .map_err(|e| format!("Failed to write temp file '{}': {e}", temp_path.display()))?;
    file.flush()
        .map_err(|e| format!("Failed to flush temp file '{}': {e}", temp_path.display()))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync temp file '{}': {e}", temp_path.display()))?;
    drop(file);

    replace_file(&temp_path, path)
}

fn replace_file(temp_path: &Path, target_path: &Path) -> Result<(), String> {
    match fs::rename(temp_path, target_path) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            if target_path.exists() {
                fs::remove_file(target_path).map_err(|remove_err| {
                    format!(
                        "Failed to replace '{}' after rename error '{}': {remove_err}",
                        target_path.display(),
                        rename_err
                    )
                })?;
                fs::rename(temp_path, target_path).map_err(|e| {
                    format!(
                        "Failed to replace '{}' with temp file '{}': {e}",
                        target_path.display(),
                        temp_path.display()
                    )
                })
            } else {
                Err(format!(
                    "Failed to rename temp file '{}' to '{}': {rename_err}",
                    temp_path.display(),
                    target_path.display()
                ))
            }
        }
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot");
    path.with_file_name(format!("{file_name}.tmp.{nanos}.{counter}"))
}
