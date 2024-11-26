mod query;
mod result;

use std::collections::VecDeque;

pub use query::{SelectQueryBuilder, SelectQueryExecutor};
pub use result::{SelectResult, SelectResultExecutor};

#[derive(Debug, Clone, Copy)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Default, Clone)]
pub struct QueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub orders: VecDeque<(Order, String)>,
}
