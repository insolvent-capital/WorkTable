use std::marker::PhantomData;

use crate::select::{Order, QueryParams};
use crate::WorkTableError;

pub trait SelectQueryExecutor<'a, Row>
where Self: Sized
{
    fn execute(&self, q: SelectQueryBuilder<'a, Row, Self>) -> Result<Vec<Row>, WorkTableError>;
}

pub struct SelectQueryBuilder<'a, Row,W>
{
    table: &'a W,
    pub params: QueryParams,
    phantom_data: PhantomData<Row>
}

impl<'a, Row, W> SelectQueryBuilder<'a, Row, W>
{
    pub fn new(table: &'a W) -> Self {
        Self {
            table,
            params: QueryParams::default(),
            phantom_data: PhantomData,
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
        self.params.orders.push_back((order, column.into()));
        self
    }

    pub fn execute(self) -> Result<Vec<Row>, WorkTableError>
    where W: SelectQueryExecutor<'a, Row>
    {
        self.table.execute(self)
    }
}