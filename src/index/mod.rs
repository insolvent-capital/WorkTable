mod table_index;
mod table_index_cdc;
mod table_secondary_index;
mod unsized_node;

pub use indexset::concurrent::map::BTreeMap as IndexMap;
pub use indexset::concurrent::multimap::BTreeMultiMap as IndexMultiMap;
pub use table_index::TableIndex;
pub use table_index_cdc::TableIndexCdc;
pub use table_secondary_index::{TableSecondaryIndex, TableSecondaryIndexCdc};
pub use unsized_node::UnsizedNode;

#[derive(Debug)]
pub struct Difference<AvailableTypes> {
    pub old: AvailableTypes,
    pub new: AvailableTypes,
}
