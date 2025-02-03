use std::sync::Arc;

use data_bucket::Link;
use scc::TreeIndex;

use crate::prelude::LockFreeSet;

mod table_secondary_index;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use table_secondary_index::TableSecondaryIndex;

pub enum IndexType<'a, T> {
    Unique(&'a TreeIndex<T, Link>),
    NonUnique(&'a TreeIndex<T, Arc<LockFreeSet<Link>>>),
}
