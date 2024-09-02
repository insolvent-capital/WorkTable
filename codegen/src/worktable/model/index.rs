use proc_macro2::Ident;

#[derive(Debug, Clone, PartialEq)]
pub struct Index {
    pub name: Ident,
    pub field: Ident,
    pub is_unique: bool
}
