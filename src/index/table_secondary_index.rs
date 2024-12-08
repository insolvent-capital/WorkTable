use data_bucket::Link;

use crate::WorkTableError;

pub trait TableSecondaryIndex<Row> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;
}

impl<Row> TableSecondaryIndex<Row> for () {
    fn save_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn delete_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }
}
