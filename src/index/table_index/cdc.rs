use std::fmt::Debug;
use std::hash::Hash;

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;

use crate::{IndexMap, IndexMultiMap};

pub trait TableIndexCdc<T> {
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>);
    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>>;
    #[allow(clippy::type_complexity)]
    fn remove_cdc(
        &self,
        value: T,
        link: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>);
}

impl<T, Node> TableIndexCdc<T> for IndexMultiMap<T, Link, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<MultiPair<T, Link>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.insert_cdc(value, link);
        (res, evs.into_iter().map(Into::into).collect())
    }

    // TODO: refactor this to be more straightforward
    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        let (res, evs) = self.insert_cdc(value, link);
        if res.is_some() {
            None
        } else {
            Some(evs.into_iter().map(Into::into).collect())
        }
    }

    fn remove_cdc(
        &self,
        value: T,
        link: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.remove_cdc(&value, &link);
        (res, evs.into_iter().map(Into::into).collect())
    }
}

impl<T, Node> TableIndexCdc<T> for IndexMap<T, Link, Node>
where
    T: Debug + Eq + Hash + Clone + Send + Ord,
    Node: NodeLike<Pair<T, Link>> + Send + 'static,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        self.insert_cdc(value, link)
    }

    fn insert_checked_cdc(&self, value: T, link: Link) -> Option<Vec<ChangeEvent<Pair<T, Link>>>> {
        self.checked_insert_cdc(value, link)
    }

    fn remove_cdc(
        &self,
        value: T,
        _: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        self.remove_cdc(&value)
    }
}
