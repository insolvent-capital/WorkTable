use std::ops::RangeBounds;

use data_bucket::Link;

mod tree_index;
mod index_set;
mod bplus_tree;

pub use index_set::{KeyValue, IndexSet};

pub trait TableIndex<K> {
    fn insert(&self, key: K, link: Link) -> Result<(), (K, Link)>;
    fn peek(&self, key: &K) -> Option<Link>;
    fn remove(&self, key: &K) -> bool;
    fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a;
    fn range<'a, R: RangeBounds<K>>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (&'a K, &'a Link)>
    where
        K: 'a;
}