use std::hash::Hash;

use data_bucket::Link;

use crate::{IndexMap, IndexMultiMap};

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn remove(&self, value: T, link: Link) -> Option<(T, Link)>;
}

impl<T> TableIndex<T> for IndexMultiMap<T, Link>
where
    T: Eq + Hash + Clone + Send + Ord,
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
    T: Eq + Hash + Clone + Send + Ord,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, _: Link) -> Option<(T, Link)> {
        self.remove(&value)
    }
}
