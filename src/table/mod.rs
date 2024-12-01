pub mod select;

use data_bucket::{Link, INNER_PAGE_SIZE};
use derive_more::{Display, Error, From};
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{Archive, Deserialize, Serialize};
use scc::ebr::Guard;
use scc::tree_index::TreeIndex;

use crate::in_memory::{DataPages, RowWrapper, StorableRow};
use crate::lock::LockMap;
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
use crate::{in_memory, TableIndex, TableRow};

#[derive(Debug)]
pub struct WorkTable<
    Row,
    Pk,
    I = (),
    PkGen = <Pk as TablePrimaryKey>::Generator,
    const DATA_LENGTH: usize = INNER_PAGE_SIZE,
> where
    Pk: Clone + Ord + 'static,
    Row: StorableRow,
{
    pub data: DataPages<Row, DATA_LENGTH>,

    pub pk_map: TreeIndex<Pk, Link>,

    pub indexes: I,

    pub pk_gen: PkGen,

    pub lock_map: LockMap,

    pub table_name: &'static str,
}

// Manual implementations to avoid unneeded trait bounds.
impl<Row, Pk, I, PkGen, const DATA_LENGTH: usize> Default
    for WorkTable<Row, Pk, I, PkGen, DATA_LENGTH>
where
    Pk: Clone + Ord + TablePrimaryKey,
    I: Default,
    PkGen: Default,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self {
            data: DataPages::new(),
            pk_map: TreeIndex::new(),
            indexes: I::default(),
            pk_gen: Default::default(),
            lock_map: LockMap::new(),
            table_name: "",
        }
    }
}

impl<Row, Pk, I, PkGen, const DATA_LENGTH: usize> WorkTable<Row, Pk, I, PkGen, DATA_LENGTH>
where
    Row: TableRow<Pk>,
    Pk: Clone + Ord + TablePrimaryKey,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn get_next_pk(&self) -> Pk
    where
        PkGen: PrimaryKeyGenerator<Pk>,
    {
        self.pk_gen.next()
    }

    /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub fn select(&self, pk: Pk) -> Option<Row>
    where
        Row: Archive,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived: Deserialize<
            <Row as StorableRow>::WrappedRow,
            rkyv::de::deserializers::SharedDeserializeMap,
        >,
    {
        let guard = Guard::new();
        let link = self.pk_map.peek(&pk, &guard)?;
        self.data.select(*link).ok()
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub fn insert<const ROW_SIZE_HINT: usize>(&self, row: Row) -> Result<Pk, WorkTableError>
    where
        Row: Archive + Serialize<AllocSerializer<ROW_SIZE_HINT>> + Clone,
        <Row as StorableRow>::WrappedRow: Archive + Serialize<AllocSerializer<ROW_SIZE_HINT>>,
        Pk: Clone,
        I: TableIndex<Row>,
    {
        let pk = row.get_primary_key().clone();
        let link = self
            .data
            .insert::<ROW_SIZE_HINT>(row.clone())
            .map_err(WorkTableError::PagesError)?;
        self.pk_map
            .insert(pk.clone(), link)
            .map_err(|_| WorkTableError::AlreadyExists)?;
        self.indexes.save_row(row, link)?;

        Ok(pk)
    }
}

#[derive(Debug, Display, Error, From)]
pub enum WorkTableError {
    NotFound,
    AlreadyExists,
    SerializeError,
    PagesError(in_memory::PagesExecutionError),
}

#[cfg(test)]
mod tests {
    // mod eyre {
    //     use eyre::*;
    //     use worktable_codegen::worktable;
    //
    //     use crate::prelude::*;
    //
    //     worktable! (
    //         name: Test,
    //         columns: {
    //             id: u64 primary_key,
    //             test: u64
    //         }
    //     );
    //
    //     #[test]
    //     fn test() {
    //         let table = TestWorkTable::default();
    //         let row = TestRow {
    //             id: 1,
    //             test: 1,
    //         };
    //         let pk = table.insert::<{ crate::table::tests::tuple_primary_key::TestRow::ROW_SIZE }>(row.clone()).unwrap();
    //         let selected_row = table.select(pk).unwrap();
    //
    //         assert_eq!(selected_row, row);
    //         assert!(table.select((1, 0).into()).is_none())
    //     }
    // }
}
