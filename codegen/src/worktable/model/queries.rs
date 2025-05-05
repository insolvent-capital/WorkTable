use std::collections::HashMap;

use proc_macro2::Ident;

use crate::worktable::model::Operation;

#[derive(Debug, Default)]
pub struct Queries {
    pub updates: HashMap<Ident, Operation>,
    pub deletes: HashMap<Ident, Operation>,
    pub in_place: HashMap<Ident, Operation>,
}
