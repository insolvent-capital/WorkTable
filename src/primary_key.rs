use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};

pub trait TablePrimaryKey {
    type Generator;
}

pub trait PrimaryKeyGenerator<T> {
    fn next(&self) -> T;
}

impl<T> PrimaryKeyGenerator<T> for AtomicU32
where T: From<u32>
{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}

impl<T> PrimaryKeyGenerator<T> for AtomicU64
where T: From<u64>{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}

impl<T> PrimaryKeyGenerator<T> for AtomicI64
where T: From<i64>{
    fn next(&self) -> T {
        self.fetch_add(1, Ordering::Relaxed).into()
    }
}
