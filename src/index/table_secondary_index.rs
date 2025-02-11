use data_bucket::Link;
use indexset::concurrent::map::BTreeMap as IndexMap;
use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use crate::WorkTableError;

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

pub trait TableSecondaryIndex<Row, AvailableTypes>
where
    AvailableTypes: 'static,
{
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_difference(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError>;
}

impl<Row, AvailableTypes> TableSecondaryIndex<Row, AvailableTypes> for ()
where
    AvailableTypes: 'static,
{
    fn save_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn delete_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn process_difference(
        &self,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError> {
        Ok(())
    }
}
