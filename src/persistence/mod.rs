mod engine;
mod manager;
mod operation;
mod space;
mod task;

pub use engine::PersistenceEngine;
pub use manager::PersistenceConfig;
pub use operation::{DeleteOperation, InsertOperation, Operation, UpdateOperation};
pub use space::{
    map_index_pages_to_toc_and_general, IndexTableOfContents, SpaceData, SpaceDataOps, SpaceIndex,
    SpaceIndexOps, SpaceSecondaryIndexOps,
};
pub use task::PersistenceTask;

pub trait PersistenceEngineOps<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents> {
    fn apply_operation(
        &mut self,
        op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryIndexEvents>,
    ) -> eyre::Result<()>;
}
