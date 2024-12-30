use std::fmt::Debug;
use std::ops::RangeBounds;

use crate::TableIndex;

impl<K, V> TableIndex<K, V> for indexset::concurrent::map::BTreeMap<K, V>
where
    K: Debug + Clone + Ord + Send + Sync + 'static,
    V: Debug + Clone + Send + Sync + Default + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        if let Some(v) = indexset::concurrent::map::BTreeMap::insert(self, key.clone(), value) {
            Err((key, v))
        } else {
            Ok(())
        }
    }

    fn peek(&self, key: &K) -> Option<V> {
        indexset::concurrent::map::BTreeMap::get(self, key).map(|kv| kv.get().value.clone())
    }

    fn remove(&self, key: &K) -> bool {
        indexset::concurrent::map::BTreeMap::remove(self, key).is_some()
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        indexset::concurrent::map::BTreeMap::iter(self)
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        indexset::concurrent::map::BTreeMap::range(self, range)
    }
}
