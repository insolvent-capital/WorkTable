use proc_macro2::{Ident, TokenStream};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub ident: Ident,
    pub vals: HashMap<Ident, TokenStream>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeneratorType {
    None,
    Autoincrement,
    Custom,
}
