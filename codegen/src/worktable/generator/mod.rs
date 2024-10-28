mod index;
mod primary_key;
mod row;
mod table;
mod wrapper;
mod queries;

use proc_macro2::Ident;

use crate::worktable::model::{Columns, Config, PrimaryKey, Queries};

pub struct Generator {
    pub name: Ident,
    pub table_name: Option<Ident>,
    pub row_name: Option<Ident>,
    pub wrapper_name: Option<Ident>,
    pub index_name: Option<Ident>,
    pub pk: Option<PrimaryKey>,
    pub queries: Option<Queries>,
    pub config: Option<Config>,

    pub columns: Columns,
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
            config: None,
            columns,
        }
    }
}
