mod index;
mod primary_key;
mod row;
mod table;
mod wrapper;
mod queries;

use proc_macro2::Ident;

use crate::worktable::model::{Columns, PrimaryKey, Queries};

pub struct Generator {
    name: Ident,
    table_name: Option<Ident>,
    row_name: Option<Ident>,
    wrapper_name: Option<Ident>,
    index_name: Option<Ident>,
    pk: Option<PrimaryKey>,
    pub queries: Option<Queries>,

    columns: Columns,
}

impl Generator {
    pub fn new(name: Ident, columns: Columns) -> Self {
        Self {
            name,
            table_name: None,
            row_name: None,
            wrapper_name: None,
            index_name: None,
            pk: None,
            queries: None,
            columns,
        }
    }
}
