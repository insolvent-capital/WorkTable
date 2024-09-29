use std::marker::PhantomData;

use crate::WorkTableError;

pub enum Order {
    Asc,
    Desc,
}

pub trait SelectQueryExecutor<'a, Row>
where Self: Sized
{
    fn execute(&self, q: SelectQueryBuilder<'a, Row, Self>) -> Result<Vec<Row>, WorkTableError>;
}

pub struct SelectQueryBuilder<'a, Row,W>
{
    table: &'a W,
    pub limit: Option<usize>,
    pub order: Order,
    pub column: String,
    phantom_data: PhantomData<Row>
}

impl<'a, Row, W> SelectQueryBuilder<'a, Row, W>
{
    pub fn new(table: &'a W) -> Self {
        Self {
            table,
            limit: None,
            order: Order::Asc,
            column: String::new(),
            phantom_data: PhantomData,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn order_by<S: Into<String>>(mut self, order: Order, column: S) -> Self {
        self.order = order;
        self.column = column.into();
        self
    }

    pub fn execute(self) -> Result<Vec<Row>, WorkTableError>
    where W: SelectQueryExecutor<'a, Row>
    {
        self.table.execute(self)
    }
}