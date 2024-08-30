use crate::in_memory::page::Link;

pub trait TableIndex<Row>
{
    fn save_row(&self, row: Row, link: Link);
}

impl<Row> TableIndex<Row> for () {
    fn save_row(&self, _: Row, _: Link) {}
}