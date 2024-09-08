use std::collections::HashMap;
use proc_macro2::Ident;

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub ident: Ident,
    pub vals: HashMap<Ident, Ident>,
}
