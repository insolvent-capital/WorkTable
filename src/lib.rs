pub mod in_memory;
mod index;
pub mod lock;
mod primary_key;
mod row;
mod table;
pub use data_bucket;
mod mem_stat;
mod persistence;
mod util;

// mod ty;
// mod value;
//
// pub use column::*;
// pub use field::*;
pub use index::*;
pub use row::*;
pub use table::*;

pub use worktable_codegen::worktable;

pub mod prelude {
    pub use crate::in_memory::{Data, DataPages, GhostWrapper, RowWrapper, StorableRow};
    pub use crate::lock::LockMap;
    pub use crate::lock::{Lock, RowLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::persistence::{
        map_index_pages_to_toc_and_general, map_unsized_index_pages_to_toc_and_general,
        validate_events, DeleteOperation, IndexTableOfContents, InsertOperation, Operation,
        OperationId, PersistenceConfig, PersistenceEngine, PersistenceEngineOps, PersistenceTask,
        SpaceData, SpaceDataOps, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized,
        SpaceSecondaryIndexOps, UpdateOperation,
    };
    pub use crate::primary_key::{PrimaryKeyGenerator, PrimaryKeyGeneratorState, TablePrimaryKey};
    pub use crate::table::select::{Order, QueryParams, SelectQueryBuilder, SelectQueryExecutor};
    pub use crate::table::system_info::{IndexInfo, IndexKind, SystemInfo};
    pub use crate::util::{OrderedF32Def, OrderedF64Def};
    pub use crate::{
        AvailableIndex, Difference, IndexError, IndexMap, IndexMultiMap, MultiPairRecreate,
        TableIndex, TableIndexCdc, TableRow, TableSecondaryIndex, TableSecondaryIndexCdc,
        TableSecondaryIndexEventsOps, TableSecondaryIndexInfo, UnsizedNode, WorkTable,
        WorkTableError,
    };
    pub use data_bucket::{
        align, get_index_page_size_from_data_length, map_data_pages_to_general, parse_data_page,
        parse_page, persist_page, seek_to_page_start, update_at, DataPage, GeneralHeader,
        GeneralPage, IndexPage, Interval, Link, PageType, Persistable, PersistableIndex,
        SizeMeasurable, SizeMeasure, SpaceInfoPage, TableOfContentsPage, UnsizedIndexPage,
        VariableSizeMeasurable, VariableSizeMeasure, DATA_VERSION, GENERAL_HEADER_SIZE,
        INNER_PAGE_SIZE, PAGE_SIZE,
    };
    pub use derive_more::{Display as MoreDisplay, From, Into};
    pub use indexset::{
        cdc::change::{ChangeEvent as IndexChangeEvent, Id as IndexChangeEventId},
        core::{multipair::MultiPair as IndexMultiPair, pair::Pair as IndexPair},
    };
    pub use ordered_float::OrderedFloat;
    pub use parking_lot::RwLock as ParkingRwLock;
    pub use worktable_codegen::{MemStat, PersistIndex, PersistTable};

    pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
    pub const WT_DATA_EXTENSION: &str = ".wt.data";
}
