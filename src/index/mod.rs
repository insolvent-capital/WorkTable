mod multipair;
mod table_index;
mod table_secondary_index;
mod unsized_node;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use multipair::MultiPairRecreate;
pub use table_index::{TableIndex, TableIndexCdc};
pub use table_secondary_index::{
    IndexError, TableSecondaryIndex, TableSecondaryIndexCdc, TableSecondaryIndexEventsOps,
    TableSecondaryIndexInfo,
};
pub use unsized_node::UnsizedNode;

#[derive(Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}
