use std::hash::Hash;
use std::sync::Arc;

use data_bucket::Link;
use scc::TreeIndex;

use crate::prelude::LockFreeSet;

mod table_secondary_index;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use table_secondary_index::TableSecondaryIndex;

pub enum IndexType<'a, T> {
    Unique(&'a TreeIndex<T, Link>),
    NonUnique(&'a TreeIndex<T, Arc<LockFreeSet<Link>>>),
}

#[derive(Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn remove(&self, value: T, link: Link) -> Option<(T, Link)>;
}

impl<T> TableIndex<T> for IndexMultiMap<T, Link>
where
    T: Eq + Hash + Clone + std::marker::Send + std::cmp::Ord,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, link: Link) -> Option<(T, Link)> {
        self.remove(&value, &link)
    }
}

impl<T> TableIndex<T> for IndexMap<T, Link>
where
    T: Eq + Hash + Clone + std::marker::Send + std::cmp::Ord,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, _link: Link) -> Option<(T, Link)> {
        self.remove(&value)
    }
}
