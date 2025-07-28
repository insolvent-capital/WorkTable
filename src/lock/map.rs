use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::lock::RowLock;

#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey> {
    map: RwLock<HashMap<PrimaryKey, Arc<tokio::sync::RwLock<LockType>>>>,
    next_id: AtomicU16,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey> {
    fn default() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            next_id: AtomicU16::default(),
        }
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: Hash + Eq + Debug + Clone,
{
    pub fn insert(
        &self,
        key: PrimaryKey,
        lock: Arc<tokio::sync::RwLock<LockType>>,
    ) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.write().insert(key, lock)
    }

    pub fn get(&self, key: &PrimaryKey) -> Option<Arc<tokio::sync::RwLock<LockType>>> {
        self.map.read().get(key).cloned()
    }

    pub fn remove(&mut self, key: &PrimaryKey) {
        self.map.write().remove(key);
    }

    #[allow(clippy::await_holding_lock)]
    pub async fn remove_with_lock_check(&self, key: &PrimaryKey)
    where
        LockType: RowLock,
    {
        let mut set = self.map.write();
        if let Some(lock) = set.get(key).cloned() {
            if let Ok(guard) = lock.try_read() {
                if !guard.is_locked() {
                    set.remove(key);
                }
            }
        }
    }

    pub fn next_id(&self) -> u16 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
