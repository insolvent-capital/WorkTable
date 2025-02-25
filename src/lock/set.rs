use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use lockfree::map::Map;

#[derive(Debug)]
pub struct LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + Ord,
{
    set: Map<PkType, Option<Arc<LockType>>>,
    next_id: AtomicU16,
}

impl<LockType, PkType> Default for LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<LockType, PkType> LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + Ord,
{
    pub fn new() -> Self {
        Self {
            set: Map::new(),
            next_id: AtomicU16::default(),
        }
    }

    pub fn insert(&self, key: PkType, lock: Arc<LockType>) {
        self.set.insert(key, Some(lock));
    }

    pub fn get(&self, key: &PkType) -> Option<Arc<LockType>> {
        self.set.get(key).map(|v| v.val().clone())?
    }

    pub fn remove(&self, key: &PkType) {
        self.set.remove(key);
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
