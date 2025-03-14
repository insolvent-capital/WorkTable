use std::collections::VecDeque;

mod query;

pub use query::{SelectQueryBuilder, SelectQueryExecutor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

type Column = String;

#[derive(Debug, Default, Clone)]
pub struct QueryParams<ColumnRange> {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order: VecDeque<(Order, Column)>,
    pub range: VecDeque<(ColumnRange, Column)>,
}
