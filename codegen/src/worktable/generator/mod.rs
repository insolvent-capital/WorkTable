mod index;
mod primary_key;
mod queries;
mod row;
mod table;
mod wrapper;

use proc_macro2::Ident;

use crate::worktable::model::{Columns, Config, PrimaryKey, Queries};

pub struct Generator {
    pub name: Ident,
    pub is_persist: bool,
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
    pub fn new(name: Ident, is_persist: bool, columns: Columns) -> Self {
        Self {
            name,
            is_persist,
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
