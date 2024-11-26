use std::sync::Arc;

use scc::TreeIndex;

use crate::prelude::Link;
use crate::prelude::LockFreeSet;
use crate::WorkTableError;

pub trait TableIndex<Row> {
    fn save_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;

    fn delete_row(&self, row: Row, link: Link) -> Result<(), WorkTableError>;
}

impl<Row> TableIndex<Row> for () {
    fn save_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }

    fn delete_row(&self, _: Row, _: Link) -> Result<(), WorkTableError> {
        Ok(())
    }
}

pub enum IndexType<'a, T> {
    Unique(&'a TreeIndex<T, Link>),
    NonUnique(&'a TreeIndex<T, Arc<LockFreeSet<Link>>>),
}
