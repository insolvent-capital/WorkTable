pub mod in_memory;
mod index;
pub mod lock;
mod primary_key;
mod row;
mod table;
pub use data_bucket as persistence;
mod database;

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
    pub use crate::database::DatabaseManager;
    pub use crate::in_memory::{ArchivedRow, Data, DataPages, RowWrapper, StorableRow};
    pub use crate::lock::LockMap;
    pub use crate::primary_key::{PrimaryKeyGenerator, PrimaryKeyGeneratorState, TablePrimaryKey};
    pub use crate::table::select::{
        Order, SelectQueryBuilder, SelectQueryExecutor, SelectResult, SelectResultExecutor,
    };
    pub use crate::{
        lock::Lock, IndexSet, KeyValue, LockFreeMap, LockedHashMap, TableIndex, TableRow,
        TableSecondaryIndex, WorkTable, WorkTableError,
    };
    pub use data_bucket::{
        align, map_data_pages_to_general, map_index_pages_to_general, map_tree_index,
        map_unique_tree_index, parse_data_page, parse_page, persist_page, DataPage, GeneralHeader,
        GeneralPage, IndexData, Interval, Link, PageType, Persistable, PersistableIndex,
        SizeMeasurable, SizeMeasure, SpaceInfoData, DATA_VERSION, GENERAL_HEADER_SIZE,
        INNER_PAGE_SIZE, PAGE_SIZE,
    };

    pub use derive_more::{From, Into};
    pub use lockfree::set::Set as LockFreeSet;
    pub use scc::{ebr::Guard, tree_index::TreeIndex};
    pub use worktable_codegen::{PersistIndex, PersistTable};
}
