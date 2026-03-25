use std::sync::atomic::{AtomicBool, Ordering};

static INTERRUPT_CHECKPOINT_AFTER_TABLES: AtomicBool = AtomicBool::new(false);

pub(crate) fn should_interrupt_checkpoint_after_tables() -> bool {
    INTERRUPT_CHECKPOINT_AFTER_TABLES.load(Ordering::SeqCst)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn enable_interrupt_checkpoint_after_tables() {
    INTERRUPT_CHECKPOINT_AFTER_TABLES.store(true, Ordering::SeqCst);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn disable_interrupt_checkpoint_after_tables() {
    INTERRUPT_CHECKPOINT_AFTER_TABLES.store(false, Ordering::SeqCst);
}
