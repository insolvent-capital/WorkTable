use data_bucket::Link;
use std::collections::HashMap;
use std::fmt::Debug;

use crate::WorkTableError;

#[derive(Debug)]
pub struct Difference<AvailableTypes> {
    pub old_value: AvailableTypes,
    pub new_value: AvailableTypes,
}

pub trait TableSecondaryIndex<Row, AvailableTypes>
where
    AvailableTypes: 'static,
{
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_difference(
        &self,
        row: Row,
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
        _: Row,
        _: Link,
        _: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<(), WorkTableError> {
        Ok(())
    }
}
