use std::collections::VecDeque;

use crate::WorkTableError;
use crate::select::{Order, QueryParams};

pub struct SelectQueryBuilder<Row, I, ColumnRange, RowFields>
where
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    pub params: QueryParams<ColumnRange, RowFields>,
    pub iter: I,
}

impl<Row, I, ColumnRange, RowFields> SelectQueryBuilder<Row, I, ColumnRange, RowFields>
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

    pub fn order_on(mut self, column: RowFields, order: Order) -> Self {
        self.params.order.push_back((order, column));
        self
    }

    pub fn range_on<R>(mut self, column: RowFields, range: R) -> Self
    where
        R: Into<ColumnRange>,
    {
        self.params.range.push_back((range.into(), column));
        self
    }
}

pub trait SelectQueryExecutor<Row, I, ColumnRange, RowFields>
where
    Self: Sized,
    I: DoubleEndedIterator<Item = Row> + Sized,
{
    fn execute(self) -> Result<Vec<Row>, WorkTableError>;
    fn where_by<F>(
        self,
        predicate: F,
    ) -> SelectQueryBuilder<Row, impl DoubleEndedIterator<Item = Row> + Sized, ColumnRange, RowFields>
    where
        F: FnMut(&Row) -> bool;
}
