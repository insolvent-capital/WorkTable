use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use lockfree::map::Map;

use crate::lock::{Lock, LockId};

#[derive(Debug)]
pub struct LockMap {
    set: Map<LockId, Arc<Lock>>,

    next_id: AtomicU16,
}

impl LockMap {
    pub fn new() -> Self {
        Self {
            set: Map::new(),
            next_id: AtomicU16::default(),
        }
    }

    pub fn insert(&self, id: LockId, lock: Arc<Lock>) {
        self.set.insert(id, lock);
    }

    pub fn get(&self, id: &LockId) -> Option<Arc<Lock>> {
        self.set.get(id).map(|v| v.val().clone())
    }

    pub fn remove(&self, id: &LockId) {
        self.set.remove(id);
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
