use std::hash::Hash;

use crate::{IndexMap, IndexMultiMap};
use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn remove(&self, value: T, link: Link) -> Option<(T, Link)>;
}

impl<T, Node> TableIndex<T> for IndexMultiMap<T, Link, Node>
where
    T: Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<MultiPair<T, Link>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, link: Link) -> Option<(T, Link)> {
        self.remove(&value, &link)
    }
}

impl<T, Node> TableIndex<T> for IndexMap<T, Link, Node>
where
    T: Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, _: Link) -> Option<(T, Link)> {
        self.remove(&value)
    }
}
