use std::collections::BTreeMap;

use derive_more::{Display, Error, From};
use crate::in_memory::DataPages;
use crate::in_memory::page::Link;
use crate::TableRow;

pub struct WorkTable<Row, Pk, I = ()> {
    rows: DataPages<Row>,

    pk_map: BTreeMap<Pk, Link>,

    indexes: Option<I>,
}

// Manual implementations to avoid unneeded trait bounds.
impl<Row, Pk, I> Default for WorkTable<Row, Pk, I> {
    fn default() -> Self {
        Self {
            rows: DataPages::new(),
            pk_map: BTreeMap::new(),
            indexes: None,
        }
    }
}

impl<Row, Pk, I> WorkTable<Row, Pk, I>
where
    Row: TableRow<Pk>,
    Pk: Ord,
{
    // /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    // pub fn select(&self, pk: Pk) -> Option<Row>
    // where
    //     Row: Clone,
    // {
    //     let link = *self.pk_map.get(&pk)?;
    //
    //     self.rows.select(link).ok()
    // }

    // /// Updates provided `Row` in table. Errors if `Row` with provided primary key was not found.
    // pub fn update(&mut self, row: Row) -> Result<Row, ExecutionError> {
    //     let pk = row.get_primary_key();
    //     let index = self.pk_map.get(pk).ok_or(ExecutionError::NotFound)?;
    //     let old_value = self.rows.remove(*index);
    //     self.rows.insert(*index, row);
    //
    //     Ok(old_value)
    // }
}

#[derive(Debug, Display, Error, From)]
pub enum ExecutionError {
    NotFound,
}
