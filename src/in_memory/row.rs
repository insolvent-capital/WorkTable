use std::fmt::Debug;

use rkyv::Archive;

/// Common trait for the `Row`s that can be stored on the [`Data`] page.
///
/// [`Data`]: crate::in_memory::data::Data
pub trait StorableRow {
    type WrappedRow: Archive + Debug;
}

pub trait RowWrapper<Inner> {
    fn get_inner(self) -> Inner;
    fn is_ghosted(&self) -> bool;
    fn from_inner(inner: Inner) -> Self;
}

pub trait GhostWrapper {
    fn unghost(&mut self);
}

pub trait Query<Row> {
    fn merge(self, row: Row) -> Row;
}
