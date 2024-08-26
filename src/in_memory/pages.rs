use crate::in_memory::page;

pub struct Pages<Row> {
    pages: Vec<page::Data<Row>>
}
