pub mod in_memory;
mod row;
mod table;
mod index;
mod primary_key;

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
    pub use crate::{TableIndex, WorkTable, in_memory::page::Link, TableRow, primary_key::PrimaryKeyGenerator, WorkTableError};
    pub use scc::{tree_index::TreeIndex, ebr::Guard};
}