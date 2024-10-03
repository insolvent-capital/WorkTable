use std::marker::PhantomData;
use crate::prelude::Order;
use crate::select::QueryParams;

pub trait SelectResultExecutor<Row>
where Self: Sized
{
    fn execute(q: SelectResult<Row, Self>) -> Vec<Row>;
}

pub struct SelectResult<Row, W> {
    pub vals: Vec<Row>,
    pub params: QueryParams,
    _phantom: PhantomData<W>,
}

impl<Row, W> SelectResult<Row, W>
where W: SelectResultExecutor<Row>
{
    pub fn new(vals: Vec<Row>) -> Self {
        Self {
            vals,
            params: Default::default(),
            _phantom: PhantomData,
        }
    }

    pub fn with_params(mut self, params: QueryParams) -> Self {
        self.params = params;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.params.limit = Some(limit);
        self
    }

    pub fn order_by<S: Into<String>>(mut self, order: Order, column: S) -> Self {
        self.params.orders.push_back((order, column.into()));
        self
    }

    pub fn execute(self) -> Vec<Row> {
        W::execute(self)
    }
}