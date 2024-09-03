mod index;
mod row;
mod table;
mod primary_key;
mod wrapper;

use proc_macro2::Ident;

use crate::worktable::model::{Columns, PrimaryKey};

pub struct Generator {
    name: Ident,
    table_name: Option<Ident>,
    row_name: Option<Ident>,
    wrapper_name: Option<Ident>,
    index_name: Option<Ident>,
    pk: Option<PrimaryKey>,

    columns: Columns
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
            columns
        }
    }
}