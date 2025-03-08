use std::hash::Hash;

use crate::{IndexMap, IndexMultiMap};
use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

pub trait TableIndexCdc<T> {
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>);
    #[allow(clippy::type_complexity)]
    fn remove_cdc(
        &self,
        value: T,
        link: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>);
}

impl<T> TableIndexCdc<T> for IndexMultiMap<T, Link>
where
    T: Eq + Hash + Clone + Send + Ord,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        let (res, evs) = self.insert_cdc(value, link);
        (res, evs.into_iter().map(Into::into).collect())
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

impl<T> TableIndexCdc<T> for IndexMap<T, Link>
where
    T: Eq + Hash + Clone + Send + Ord,
{
    fn insert_cdc(&self, value: T, link: Link) -> (Option<Link>, Vec<ChangeEvent<Pair<T, Link>>>) {
        self.insert_cdc(value, link)
    }

    fn remove_cdc(
        &self,
        value: T,
        _: Link,
    ) -> (Option<(T, Link)>, Vec<ChangeEvent<Pair<T, Link>>>) {
        self.remove_cdc(&value)
    }
}
