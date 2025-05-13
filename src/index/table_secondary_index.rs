use std::collections::HashMap;

use data_bucket::Link;

use crate::system_info::IndexInfo;
use crate::Difference;
use crate::WorkTableError;

pub trait TableSecondaryIndex<Row, AvailableTypes, AvailableIndexes> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), IndexError<AvailableIndexes>>;

    fn delete_from_indexes(
        &self,
        row: Row,
        link: Link,
        indexes: Vec<AvailableIndexes>,
    ) -> Result<(), IndexError<AvailableIndexes>>;

    fn process_difference(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError>;

    fn index_info(&self) -> Vec<IndexInfo>;
}

pub trait TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes> {
    fn save_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn delete_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn process_difference_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, WorkTableError>;
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

    fn process_difference(
        &self,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn index_info(&self) -> Vec<IndexInfo> {
        vec![]
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

impl<IndexNameEnum> From<IndexError<IndexNameEnum>> for WorkTableError {
    fn from(value: IndexError<IndexNameEnum>) -> Self {
        match value {
            IndexError::AlreadyExists { .. } => WorkTableError::AlreadyExists,
            IndexError::NotFound => WorkTableError::NotFound,
        }
    }
}
