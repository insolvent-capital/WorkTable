use std::fs;
use std::marker::PhantomData;
use std::path::Path;

use crate::persistence::operation::Operation;
use crate::persistence::{
    PersistenceEngineOps, SpaceDataOps, SpaceIndexOps, SpaceSecondaryIndexOps,
};
use crate::prelude::{PrimaryKeyGeneratorState, TablePrimaryKey};

#[derive(Debug)]
pub struct PersistenceEngine<
    SpaceData,
    SpacePrimaryIndex,
    SpaceSecondaryIndexes,
    PrimaryKey,
    SecondaryIndexEvents,
    PrimaryKeyGenState = <<PrimaryKey as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
>
where
    PrimaryKey: TablePrimaryKey,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState
{
    pub data: SpaceData,
    pub primary_index: SpacePrimaryIndex,
    pub secondary_indexes: SpaceSecondaryIndexes,
    phantom_data: PhantomData<(PrimaryKey, SecondaryIndexEvents, PrimaryKeyGenState)>,
}

impl<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        PrimaryKeyGenState,
    >
    PersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        PrimaryKeyGenState,
    >
where
    PrimaryKey: Ord + TablePrimaryKey,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SpaceData: SpaceDataOps<PrimaryKeyGenState>,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey>,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents>,
{
    pub async fn from_table_files_path<S: AsRef<str> + Clone + Send>(
        path: S,
    ) -> eyre::Result<Self> {
        let table_path = Path::new(path.as_ref());
        if !table_path.exists() {
            fs::create_dir_all(table_path)?;
        }

        Ok(Self {
            data: SpaceData::from_table_files_path(path.clone()).await?,
            primary_index: SpacePrimaryIndex::primary_from_table_files_path(path.clone()).await?,
            secondary_indexes: SpaceSecondaryIndexes::from_table_files_path(path).await?,
            phantom_data: PhantomData,
        })
    }
}

impl<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        PrimaryKeyGenState,
    > PersistenceEngineOps<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>
    for PersistenceEngine<
        SpaceData,
        SpacePrimaryIndex,
        SpaceSecondaryIndexes,
        PrimaryKey,
        SecondaryIndexEvents,
        PrimaryKeyGenState,
    >
where
    PrimaryKey: Ord + TablePrimaryKey + Send,
    <PrimaryKey as TablePrimaryKey>::Generator: PrimaryKeyGeneratorState,
    SpaceData: SpaceDataOps<PrimaryKeyGenState> + Send,
    SpacePrimaryIndex: SpaceIndexOps<PrimaryKey> + Send,
    SpaceSecondaryIndexes: SpaceSecondaryIndexOps<SecondaryIndexEvents> + Send,
    SecondaryIndexEvents: Send,
    PrimaryKeyGenState: Send,
{
    async fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()> {
        match op {
            Operation::Insert(insert) => {
                self.data
                    .save_data(insert.link, insert.bytes.as_ref())
                    .await?;
                for event in insert.primary_key_events {
                    self.primary_index.process_change_event(event).await?;
                }
                let info = self.data.get_mut_info();
                info.inner.pk_gen_state = insert.pk_gen_state;
                self.data.save_info().await?;
                self.secondary_indexes
                    .process_change_events(insert.secondary_keys_events)
                    .await
            }
            Operation::Update(update) => {
                self.data
                    .save_data(update.link, update.bytes.as_ref())
                    .await?;
                self.secondary_indexes
                    .process_change_events(update.secondary_keys_events)
                    .await
            }
            Operation::Delete(delete) => {
                for event in delete.primary_key_events {
                    self.primary_index.process_change_event(event).await?;
                }
                self.secondary_indexes
                    .process_change_events(delete.secondary_keys_events)
                    .await
            }
        }
    }
}
