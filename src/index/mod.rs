use std::sync::Arc;

use data_bucket::Link;
use scc::TreeIndex;

use crate::prelude::LockFreeSet;

mod table_index;
mod table_secondary_index;

pub use table_index::{IndexSet, LockFreeMap, LockedHashMap, TableIndex};
pub use table_secondary_index::{Difference, TableSecondaryIndex};

pub enum IndexType<'a, T> {
    Unique(&'a TreeIndex<T, Link>),
    NonUnique(&'a TreeIndex<T, Arc<LockFreeSet<Link>>>),
}
