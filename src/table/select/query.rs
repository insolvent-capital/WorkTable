use std::collections::VecDeque;

use crate::select::{Order, QueryParams};
use crate::WorkTableError;

#[derive(Clone)]
pub struct SelectQueryBuilder<Row, I, ColumnRange>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub params: QueryParams<ColumnRange>,
    pub iter: I,
}

impl<Row, I, ColumnRange> SelectQueryBuilder<Row, I, ColumnRange>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub fn new(iter: I) -> Self {
        Self {
            params: QueryParams {
                limit: None,
                offset: None,
                order: VecDeque::new(),
                range: VecDeque::new(),
            },
            iter,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.params.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.params.offset = Some(offset);
        self
    }

    pub fn order_by<S: Into<String>>(mut self, order: Order, column: S) -> Self {
        self.params.order.push_back((order, column.into()));
        self
    }

    pub fn where_by<R>(mut self, range: R, column: impl Into<String>) -> Self
    where
        R: Into<ColumnRange>,
    {
        self.params.range.push_back((range.into(), column.into()));
        self
    }
}

pub trait SelectQueryExecutor<Row, I, T>
where
    Self: Sized,
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    fn execute(self) -> Result<Vec<Row>, WorkTableError>;
}
