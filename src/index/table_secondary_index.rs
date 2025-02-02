use data_bucket::Link;
use std::collections::HashMap;

use crate::WorkTableError;

#[derive(Debug)]
pub enum AvailableTypes {
    I16(i16),
    U16(u16),
    STRING(String),
}

#[derive(Debug)]
pub struct Difference {
    pub old_value: AvailableTypes,
    pub new_value: AvailableTypes,
}

pub trait TableSecondaryIndex<Row> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn process_difference(
        &self,
        row: Row,
        link: Link,
        differences: HashMap<Row, Difference>,
    ) -> Result<(), WorkTableError>;
}

impl<Row> TableSecondaryIndex<Row> for () {
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
        _: HashMap<Row, Difference>,
    ) -> Result<(), WorkTableError> {
        Ok(())
    }
}
