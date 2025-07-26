use std::collections::HashMap;

use data_bucket::Link;

use crate::{Difference, IndexError, WorkTableError};

pub trait TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents, AvailableIndexes> {
    fn save_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn reinsert_row_cdc(
        &self,
        row_old: Row,
        link_old: Link,
        row_new: Row,
        link_new: Link,
    ) -> eyre::Result<SecondaryEvents>;
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
