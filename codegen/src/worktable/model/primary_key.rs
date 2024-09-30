use std::collections::HashMap;
use proc_macro2::{Ident, TokenStream};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub ident: Ident,
    pub vals: HashMap<Ident, TokenStream>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeneratorType {
    None,
    Autoincrement,
    Custom
}