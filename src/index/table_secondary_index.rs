use std::collections::HashMap;

use data_bucket::Link;

use crate::system_info::IndexInfo;
use crate::Difference;
use crate::WorkTableError;

pub trait TableSecondaryIndex<Row, AvailableTypes> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_difference(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError>;

    fn index_info(&self) -> Vec<IndexInfo>;
}

pub trait TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents> {
    fn save_row_cdc(&self, row: Row, link: Link) -> Result<SecondaryEvents, WorkTableError>;
    fn delete_row_cdc(&self, row: Row, link: Link) -> Result<SecondaryEvents, WorkTableError>;
    fn process_difference_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, WorkTableError>;
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

    fn index_info(&self) -> Vec<IndexInfo> {
        vec![]
    }
}
