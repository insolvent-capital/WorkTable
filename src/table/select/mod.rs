use std::collections::VecDeque;

mod query;

pub use query::{SelectQueryBuilder, SelectQueryExecutor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Default, Clone)]
pub struct QueryParams<ColumnRange, RowFields> {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: VecDeque<(Order, RowFields)>,
    pub range: VecDeque<(ColumnRange, RowFields)>,
}
