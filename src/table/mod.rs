pub mod select;

use std::marker::PhantomData;

use data_bucket::{Link, INNER_PAGE_SIZE};
use derive_more::{Display, Error, From};
#[cfg(feature = "perf_measurements")]
use performance_measurement_codegen::performance_measurement;
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::in_memory::{DataPages, RowWrapper, StorableRow};
use crate::lock::LockMap;
use crate::persistence::{InsertOperation, Operation};
use crate::prelude::PrimaryKeyGeneratorState;
use crate::primary_key::{PrimaryKeyGenerator, TablePrimaryKey};
use crate::{in_memory, IndexMap, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc};

#[derive(Debug)]
pub struct WorkTable<
    Row,
    PrimaryKey,
    AvailableTypes = (),
    SecondaryIndexes = (),
    LockType = (),
    PkGen = <PrimaryKey as TablePrimaryKey>::Generator,
    const DATA_LENGTH: usize = INNER_PAGE_SIZE,
> where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow,
{
    pub data: DataPages<Row, DATA_LENGTH>,

    pub pk_map: IndexMap<PrimaryKey, Link>,

    pub indexes: SecondaryIndexes,

    pub pk_gen: PkGen,

    pub lock_map: LockMap<LockType, PrimaryKey>,

    pub table_name: &'static str,

    pub pk_phantom: PhantomData<PrimaryKey>,

    pub types_phantom: PhantomData<AvailableTypes>,
}

// Manual implementations to avoid unneeded trait bounds.
impl<
        Row,
        PrimaryKey,
        AvailableTypes,
        SecondaryIndexes,
        LockType,
        PkGen,
        const DATA_LENGTH: usize,
    > Default
    for WorkTable<Row, PrimaryKey, AvailableTypes, SecondaryIndexes, LockType, PkGen, DATA_LENGTH>
where
    PrimaryKey: Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    SecondaryIndexes: Default,
    PkGen: Default,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    fn default() -> Self {
        Self {
            data: DataPages::new(),
            pk_map: IndexMap::default(),
            indexes: SecondaryIndexes::default(),
            pk_gen: Default::default(),
            lock_map: LockMap::new(),
            table_name: "",
            pk_phantom: PhantomData,
            types_phantom: PhantomData,
        }
    }
}

impl<
        Row,
        PrimaryKey,
        AvailableTypes,
        SecondaryIndexes,
        LockType,
        PkGen,
        const DATA_LENGTH: usize,
    > WorkTable<Row, PrimaryKey, AvailableTypes, SecondaryIndexes, LockType, PkGen, DATA_LENGTH>
where
    Row: TableRow<PrimaryKey>,
    PrimaryKey: Clone + Ord + Send + TablePrimaryKey + std::hash::Hash,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn get_next_pk(&self) -> PrimaryKey
    where
        PkGen: PrimaryKeyGenerator<PrimaryKey>,
    {
        self.pk_gen.next()
    }

    /// Selects `Row` from table identified with provided primary key. Returns `None` if no value presented.
    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub fn select(&self, pk: PrimaryKey) -> Option<Row>
    where
        LockType: 'static,
        Row: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <<Row as StorableRow>::WrappedRow as Archive>::Archived:
            Deserialize<<Row as StorableRow>::WrappedRow, HighDeserializer<rkyv::rancor::Error>>,
    {
        let link = self.pk_map.get(&pk).map(|v| v.get().value)?;
        self.data.select(link).ok()
    }

    #[cfg_attr(
        feature = "perf_measurements",
        performance_measurement(prefix_name = "WorkTable")
    )]
    pub fn insert(&self, row: Row) -> Result<PrimaryKey, WorkTableError>
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        PrimaryKey: Clone,
        AvailableTypes: 'static,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes>,
        LockType: 'static,
    {
        let pk = row.get_primary_key().clone();
        let link = self
            .data
            .insert(row.clone())
            .map_err(WorkTableError::PagesError)?;
        self.pk_map
            .insert(pk.clone(), link)
            .map_or(Ok(()), |_| Err(WorkTableError::AlreadyExists))?;
        self.indexes.save_row(row, link)?;

        Ok(pk)
    }

    #[allow(clippy::type_complexity)]
    pub fn insert_cdc<SecondaryEvents>(
        &self,
        row: Row,
    ) -> Result<
        (
            PrimaryKey,
            Operation<<PkGen as PrimaryKeyGeneratorState>::State, PrimaryKey, SecondaryEvents>,
        ),
        WorkTableError,
    >
    where
        Row: Archive
            + Clone
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <Row as StorableRow>::WrappedRow: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        PrimaryKey: Clone,
        SecondaryIndexes: TableSecondaryIndex<Row, AvailableTypes>
            + TableSecondaryIndexCdc<Row, AvailableTypes, SecondaryEvents>,
        PkGen: PrimaryKeyGeneratorState,
    {
        let pk = row.get_primary_key().clone();
        let (link, bytes) = self
            .data
            .insert_cdc(row.clone())
            .map_err(WorkTableError::PagesError)?;
        let (exists, primary_key_events) = self.pk_map.insert_cdc(pk.clone(), link);
        if exists.is_some() {
            return Err(WorkTableError::AlreadyExists);
        }
        let secondary_keys_events = self.indexes.save_row_cdc(row, link)?;

        let op = Operation::Insert(InsertOperation {
            id: Default::default(),
            pk_gen_state: self.pk_gen.get_state(),
            primary_key_events,
            secondary_keys_events,
            bytes,
            link,
        });

        Ok((pk, op))
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
