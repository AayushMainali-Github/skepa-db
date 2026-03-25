use std::path::Path;

pub(crate) fn should_interrupt_checkpoint_after_tables(db_path: &Path) -> bool {
    db_path
        .join(".simulate_interrupt_checkpoint_after_tables")
        .exists()
}
