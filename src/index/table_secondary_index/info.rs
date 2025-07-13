use crate::prelude::IndexInfo;

pub trait TableSecondaryIndexInfo {
    fn index_info(&self) -> Vec<IndexInfo>;
    fn is_empty(&self) -> bool;
    fn is_unit() -> bool {
        false
    }
}

impl TableSecondaryIndexInfo for () {
    fn index_info(&self) -> Vec<IndexInfo> {
        vec![]
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn is_unit() -> bool {
        true
    }
}
