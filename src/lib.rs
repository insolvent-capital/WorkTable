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
    pub use crate::in_memory::{Data, DataPages, GhostWrapper, Query, RowWrapper, StorableRow};
    pub use crate::lock::LockMap;
    pub use crate::lock::{Lock, RowLock};
    pub use crate::mem_stat::MemStat;
    pub use crate::persistence::{
        DeleteOperation, IndexTableOfContents, InsertOperation, Operation, OperationId,
        PersistenceConfig, PersistenceEngine, PersistenceEngineOps, PersistenceTask, SpaceData,
        SpaceDataOps, SpaceIndex, SpaceIndexOps, SpaceIndexUnsized, SpaceSecondaryIndexOps,
        UpdateOperation, map_index_pages_to_toc_and_general,
        map_unsized_index_pages_to_toc_and_general, validate_events,
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
        DATA_VERSION, DataPage, GENERAL_HEADER_SIZE, GeneralHeader, GeneralPage, INNER_PAGE_SIZE,
        IndexPage, Interval, Link, PAGE_SIZE, PageType, Persistable, PersistableIndex,
        SizeMeasurable, SizeMeasure, SpaceInfoPage, TableOfContentsPage, UnsizedIndexPage,
        VariableSizeMeasurable, VariableSizeMeasure, align, get_index_page_size_from_data_length,
        map_data_pages_to_general, parse_data_page, parse_page, persist_page, seek_to_page_start,
        update_at,
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

// TODO:
// 1. add checked inserts to indexset to not insert/remove but just insert with violation error
// 2. Add pre-update state storage to avoid ghost reads of updated data if it will be rolled back
