use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

use crate::{IndexMap, IndexMultiMap};

mod cdc;

pub use cdc::TableIndexCdc;

pub trait TableIndex<T> {
    fn insert(&self, value: T, link: Link) -> Option<Link>;
    fn remove(&self, value: T, link: Link) -> Option<(T, Link)>;
}

impl<T, Node> TableIndex<T> for IndexMultiMap<T, Link, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
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
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    fn insert(&self, value: T, link: Link) -> Option<Link> {
        self.insert(value, link)
    }

    fn remove(&self, value: T, _: Link) -> Option<(T, Link)> {
        self.remove(&value)
    }
}
