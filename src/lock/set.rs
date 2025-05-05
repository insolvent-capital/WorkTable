use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

#[derive(Debug)]
pub struct LockMap<LockType, PkType>
where
    PkType: std::hash::Hash + Ord,
{
    set: RwLock<HashMap<PkType, Arc<LockType>>>,
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
            set: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
        }
    }

    pub fn insert(&self, key: PkType, lock: Arc<LockType>) -> Option<Arc<LockType>> {
        self.set.write().insert(key, lock)
    }

    pub fn get(&self, key: &PkType) -> Option<Arc<LockType>> {
        self.set.read().get(key).cloned()
    }

    pub fn remove(&self, key: &PkType) {
        self.set.write().remove(key);
    }

    pub fn remove_with_lock_check(&self, key: &PkType, lock: Arc<LockType>)
    where
        PkType: Clone,
    {
        let mut set = self.set.write();
        if let Some(l) = set.remove(key) {
            if !Arc::ptr_eq(&l, &lock) {
                set.insert(key.clone(), l);
            }
        }
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
