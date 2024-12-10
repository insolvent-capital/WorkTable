use std::ops::RangeBounds;

use data_bucket::Link;

mod bplus_tree;
mod index_set;
mod tree_index;

pub use index_set::{IndexSet, KeyValue};

pub trait TableIndex<K, V> {
    fn insert(&self, key: K, value: V) -> Result<(), (K, V)>;
    fn peek(&self, key: &K) -> Option<V>;
    fn remove(&self, key: &K) -> bool;
    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a;
    fn range<'a, R: RangeBounds<K>>(&'a self, range: R) -> impl Iterator<Item = (&'a K, &'a V)>
    where
        K: 'a,
        V: 'a;
}
