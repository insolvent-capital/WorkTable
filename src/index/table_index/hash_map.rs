use std::collections::HashMap;
use std::hash::Hash;
use std::intrinsics::transmute;
use std::ops::RangeBounds;
use std::sync::{RwLock, RwLockReadGuard};

use crate::TableIndex;

pub type LockedHashMap<K, V> = RwLock<HashMap<K, V>>;

pub struct LockedHashMapIter<'a, K, V> {
    lock: RwLockReadGuard<'a, HashMap<K, V>>,
}

impl<K, V> TableIndex<K, V> for LockedHashMap<K, V>
where
    K: Hash + Clone + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        if let Some(v) = self.write().unwrap().insert(key.clone(), value) {
            Err((key, v))
        } else {
            Ok(())
        }
    }

    fn peek(&self, key: &K) -> Option<V> {
        self.read().unwrap().get(key).cloned()
    }

    fn remove(&self, key: &K) -> bool {
        self.write().unwrap().remove(key).is_some()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        unsafe {
            transmute::<_, std::collections::hash_map::Iter<'a, _, _>>(self.read().unwrap().iter())
        }
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, _: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        TableIndex::iter(self)
    }
}
