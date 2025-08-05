mod cdc;
mod index_events;
mod info;

use data_bucket::Link;
use std::collections::HashMap;

use crate::WorkTableError;
use crate::{AvailableIndex, Difference};

pub use cdc::TableSecondaryIndexCdc;
pub use index_events::TableSecondaryIndexEventsOps;
pub use info::TableSecondaryIndexInfo;

pub trait TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>>;
    fn reinsert_row(
        &self,
        row_old: Row,
        link_old: Link,
        row_new: Row,
        link_new: Link,
    ) -> Result<(), IndexError<AvailableIndexes>>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>>;

    fn delete_from_indexes(
        &self,
        row: Row,
        link: Link,
        indexes: Vec<AvailableIndexes>,
    ) -> Result<(), IndexError<AvailableIndexes>>;

    fn process_difference_insert(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>>;

    fn process_difference_remove(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>>;
}

impl<Row, AvailableTypes, AvailableIndexes>
    TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes> for ()
where
    AvailableTypes: 'static,
    AvailableIndexes: 'static,
{
    fn save_row(&self, _: Row, _: Link) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }

    fn reinsert_row(
        &self,
        _: Row,
        _: Link,
        _: Row,
        _: Link,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }

    fn delete_row(&self, _: Row, _: Link) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }

    fn delete_from_indexes(
        &self,
        _: Row,
        _: Link,
        _: Vec<AvailableIndexes>,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }

    fn process_difference_insert(
        &self,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }

    fn process_difference_remove(
        &self,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), IndexError<AvailableIndexes>> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum IndexError<IndexNameEnum> {
    AlreadyExists {
        at: IndexNameEnum,
        inserted_already: Vec<IndexNameEnum>,
    },
    NotFound,
}

impl<IndexNameEnum> From<IndexError<IndexNameEnum>> for WorkTableError
where
    IndexNameEnum: AvailableIndex,
{
    fn from(value: IndexError<IndexNameEnum>) -> Self {
        match value {
            IndexError::AlreadyExists {
                at,
                inserted_already: _,
            } => WorkTableError::AlreadyExists(at.to_string_value()),
            IndexError::NotFound => WorkTableError::NotFound,
        }
    }
}
