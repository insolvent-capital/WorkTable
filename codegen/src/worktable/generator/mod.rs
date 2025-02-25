mod index;
mod locks;
mod primary_key;
mod queries;
mod row;
mod table;
//mod table_old;
//mod table_index;
mod wrapper;

use proc_macro2::Ident;

use crate::worktable::model::{Columns, Config, PrimaryKey, Queries};

pub struct Generator {
    pub name: Ident,
    pub is_persist: bool,
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
            pk: None,
            queries: None,
            config: None,
            columns,
        }
    }
}
