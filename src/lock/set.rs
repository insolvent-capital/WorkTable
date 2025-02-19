use std::sync::Arc;

use lockfree::map::Map;

#[derive(Debug)]
pub struct LockMap<LockType, PrimaryKey>
where
    PrimaryKey: std::hash::Hash + std::cmp::Ord,
{
    set: Map<PrimaryKey, Option<Arc<LockType>>>,
}

impl<LockType, PrimaryKey> Default for LockMap<LockType, PrimaryKey>
where
    PrimaryKey: std::hash::Hash + std::cmp::Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<LockType, PrimaryKey> LockMap<LockType, PrimaryKey>
where
    PrimaryKey: std::hash::Hash + std::cmp::Ord,
{
    pub fn new() -> Self {
        Self { set: Map::new() }
    }

    pub fn insert(&self, id: PrimaryKey, lock: Arc<LockType>) {
        self.set.insert(id, Some(lock));
    }

    pub fn get(&self, id: &PrimaryKey) -> Option<Arc<LockType>> {
        self.set.get(id).map(|v| v.val().clone())?
    }

    pub fn remove(&self, id: &PrimaryKey) {
        self.set.remove(id);
    }
}
