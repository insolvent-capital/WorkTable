use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

pub trait TablePrimaryKey {
    type Generator;
}

impl TablePrimaryKey for u32 {
    type Generator = AtomicU32;
}

impl TablePrimaryKey for u64 {
    type Generator = AtomicU64;
}

pub trait PrimaryKeyGenerator<T> {
    fn next(&self) -> T;
}

impl PrimaryKeyGenerator<u32> for AtomicU32 {
    fn next(&self) -> u32 {
        self.fetch_add(1, Ordering::Relaxed)
    }
}

impl PrimaryKeyGenerator<u64> for AtomicU64 {
    fn next(&self) -> u64 {
        self.fetch_add(1, Ordering::Relaxed)
    }
}
