use std::hash::Hash;
use std::intrinsics::transmute;
use std::ops::RangeBounds;

use crate::TableIndex;

impl<K, V> TableIndex<K, V> for lockfree::map::Map<K, V>
where
    K: Hash + Clone + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        if let Some(v) = lockfree::map::Map::insert(self, key, value) {
            Err(v.clone())
        } else {
            Ok(())
        }
    }

    fn peek(&self, key: &K) -> Option<V> {
        lockfree::map::Map::get(self, key).map(|v| v.val().clone())
    }

    fn remove(&self, key: &K) -> bool {
        lockfree::map::Map::remove(self, key).is_some()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        lockfree::map::Map::iter(self)
            .map(|r| r.clone())
            .map(|(k, v)| (unsafe { transmute(&k) }, unsafe { transmute(&v) }))
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, _: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        TableIndex::iter(self)
    }
}
