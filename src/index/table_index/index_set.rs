use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::RangeBounds;

use data_bucket::Link;

use crate::TableIndex;

pub type IndexSet<K, V> = indexset::concurrent2::set::BTreeSet<KeyValue<K, V>>;

#[derive(Copy, Clone, Debug)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> PartialEq<Self> for KeyValue<K, V>
where
    K: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> PartialOrd for KeyValue<K, V>
where
    K: PartialOrd + PartialEq,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K, V> Eq for KeyValue<K, V> where K: PartialEq + PartialOrd + Eq {}

impl<K, V> Ord for KeyValue<K, V>
where
    K: PartialOrd + PartialEq + Ord + Eq,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> Borrow<K> for KeyValue<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<K, V> TableIndex<K, V> for IndexSet<K, V>
where
    K: Debug + Clone + Ord + Send + Sync + 'static,
    V: Debug + Clone + Send + Sync + Default + 'static,
{
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)> {
        if indexset::concurrent2::set::BTreeSet::insert(
            self,
            KeyValue {
                key: key.clone(),
                value: value.clone(),
            },
        ) {
            Ok(())
        } else {
            let kv = KeyValue {
                key: key.clone(),
                value: value.clone(),
            };
            let kv = indexset::concurrent2::set::BTreeSet::get(self, &kv)
                .expect("should exist as false returned");
            Err((kv.get().key.clone(), kv.get().value.clone()))
        }
    }

    fn peek(&self, key: &K) -> Option<V> {
        indexset::concurrent2::set::BTreeSet::get(
            self,
            &KeyValue {
                key: key.clone(),
                value: Default::default(),
            },
        )
        .map(|r| r.get().value.clone())
    }

    fn remove(&self, key: &K) -> bool {
        if let Some(_) = self.peek(key) {
            false
        } else {
            indexset::concurrent2::set::BTreeSet::remove_range::<std::ops::Range<&K>, K>(
                self,
                key..key,
            );
            true
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        self.iter().map(|kv| (&kv.key, &kv.value))
    }

    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a,
    {
        self.range(range).map(|kv| (&kv.key, &kv.value))
    }
}
