use std::collections::HashMap;

use data_bucket::Link;

use crate::{Difference, IndexError};

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
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn delete_row_cdc(
        &self,
        row: Row,
        link: Link,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn process_difference_insert_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
    fn process_difference_remove_cdc(
        &self,
        link: Link,
        differences: HashMap<&str, Difference<AvailableTypes>>,
    ) -> Result<SecondaryEvents, IndexError<AvailableIndexes>>;
}
