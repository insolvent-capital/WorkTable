use std::sync::Arc;

use derive_more::{Display, Error, From};
use scc::tree_index::TreeIndex;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::ser::serializers::AllocSerializer;
use scc::ebr::Guard;
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use crate::in_memory::DataPages;
use crate::in_memory::page::Link;
use crate::{in_memory, TableIndex, TableRow};
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};

#[derive(Debug)]
pub struct WorkTable<Row, Pk, I = (), PkGen = <Pk as TablePrimaryKey>::Generator>
where Pk: Clone + Ord + 'static
{
    pub data: Arc<DataPages<Row>>,

    pk_map: TreeIndex<Pk, Link>,

    pub indexes: I,

    pk_gen: Arc<PkGen>,
}

impl<Row, Pk, I> Clone for WorkTable<Row, Pk, I>
where Pk: Clone + Ord + TablePrimaryKey,
      I: Clone,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            pk_map: self.pk_map.clone(),
            indexes: self.indexes.clone(),
            pk_gen: self.pk_gen.clone(),
        }
    }
}

// Manual implementations to avoid unneeded trait bounds.
impl<Row, Pk, I> Default for WorkTable<Row, Pk, I>
where Pk: Clone + Ord + TablePrimaryKey,
        I: Default,
        <Pk as TablePrimaryKey>::Generator: Default
{
    fn default() -> Self {
        Self {
            data: Arc::new(DataPages::new()),
            pk_map: TreeIndex::new(),
            indexes: I::default(),
            pk_gen: Arc::new(Default::default()),
        }
    }
}

impl<Row, Pk, I, PkGen> WorkTable<Row, Pk, I, PkGen>
where
    Row: TableRow<Pk>,
    Pk: Clone + Ord + TablePrimaryKey ,
    PkGen: PrimaryKeyGenerator<Pk>
{
    pub fn get_next_pk(&self) -> Pk {
        self.pk_gen.next()
    }


    /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "WorkTable"))]
    pub fn select(&self, pk: Pk) -> Option<Row>
    where
        Row: Archive,
        <Row as Archive>::Archived:Deserialize<Row, rkyv::Infallible>,
    {
        let guard = Guard::new();
        let link = self.pk_map.peek(&pk, &guard)?;
        self.data.select(*link).ok()
    }

    #[cfg_attr(feature = "perf_measurements", performance_measurement(prefix_name = "WorkTable"))]
    pub fn insert<const ROW_SIZE_HINT: usize>(&self, row: Row) -> Result<Pk, WorkTableError>
    where
        Row: Archive + Serialize<AllocSerializer<ROW_SIZE_HINT>> + Clone,
        Pk: Clone,
        I: TableIndex<Row>
    {
        let pk = row.get_primary_key().clone();
        let link = self.data.insert::<ROW_SIZE_HINT>(row.clone()).map_err(WorkTableError::PagesError)?;
        let _ = self.pk_map.insert(pk.clone(), link).map_err(|_| WorkTableError::AlreadyExists)?;
        self.indexes.save_row(row, link)?;

        Ok(pk)
    }

    // /// Updates provided `Row` in table. Errors if `Row` with provided primary key was not found.
    // pub fn update(&mut self, row: Row) -> Result<Row, WorkTableError> {
    //     let pk = row.get_primary_key();
    //     let index = self.pk_map.get(pk).ok_or(WorkTableError::NotFound)?;
    //     let old_value = self.rows.remove(*index);
    //     self.rows.insert(*index, row);
    //
    //     Ok(old_value)
    // }
}

#[derive(Debug, Display, Error, From)]
pub enum WorkTableError {
    NotFound,
    AlreadyExists,
    PagesError(in_memory::PagesExecutionError)
}

#[cfg(test)]
mod tests {
    use worktable_codegen::worktable;

    use crate::prelude::*;

    worktable! (
        name: Test,
        columns: {
            id: u64 primary_key,
            test: i64,
            exchange: String
        },
        indexes: {
            test_idx: test,
            exchnage_idx: exchange unique,
        }
    );

    #[test]
    fn bench() {
        let table = TestWorkTable::default();

        let mut v = Vec::with_capacity(10000);

        for i in 0..10000 {
            let row = TestRow {
                id: table.get_next_pk(),
                test: i + 1,
                exchange: "XD".to_string()
            };

            let a = table.insert::<24>(row).expect("TODO: panic message");
            v.push(a)
        }

        for a in v {
            table.select(a).expect("TODO: panic message");
        }
    }

    #[test]
    fn insert() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let selected_row = table.select(pk).unwrap();

        assert_eq!(selected_row, row);
        assert!(table.select(2).is_none())
    }

    #[test]
    fn insert_same() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let res = table.insert::<{ TestRow::ROW_SIZE }>(row.clone());
        assert!(res.is_err())
    }

    #[test]
    fn insert_exchange_same() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 2,
            exchange: "test".to_string()
        };
        let res = table.insert::<{ TestRow::ROW_SIZE }>(row.clone());
        assert!(res.is_err())
    }

    #[test]
    fn select_by_test() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let selected_rows = table.select_by_test(1).unwrap();

        assert_eq!(selected_rows.len(), 1);
        assert!(selected_rows.contains(&row));
        assert!(table.select_by_test(2).is_err())
    }

    #[test]
    fn select_multiple_by_test() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let row_next = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row_next.clone()).unwrap();
        let selected_rows = table.select_by_test(1).unwrap();

        assert_eq!(selected_rows.len(), 2);
        assert!(selected_rows.contains(&row));
        assert!(selected_rows.contains(&row_next));
        assert!(table.select_by_test(2).is_err())
    }


    #[test]
    fn select_by_name() {
        let table = TestWorkTable::default();
        let row = TestRow {
            id: table.get_next_pk(),
            test: 1,
            exchange: "test".to_string()
        };
        let pk = table.insert::<{ TestRow::ROW_SIZE }>(row.clone()).unwrap();
        let selected_row = table.select_by_exchange("test".to_string()).unwrap();

        assert_eq!(selected_row, row);
        assert!(table.select_by_exchange("2".to_string()).is_none())
    }
}
