use std::intrinsics::transmute;
use std::ops::RangeBounds;

use scc::ebr::Guard;

use crate::TableIndex;

impl<K, V> TableIndex<K, V> for scc::TreeIndex<K, V>
where
    K: Clone + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        scc::TreeIndex::insert(self, key, value)
    }

    fn peek(&self, key: &K) -> Option<V> {
        let guard = Guard::new();
        scc::TreeIndex::peek(self, key, &guard).cloned()
    }

    fn remove(&self, key: &K) -> bool {
        scc::TreeIndex::remove(self, key)
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::iter(self, guard)
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        let guard = Guard::new();
        let guard: &'a Guard = unsafe { transmute(&guard) };
        scc::TreeIndex::range(self, range, guard)
    }
}
