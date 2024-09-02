use proc_macro2::Ident;

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    pub ident: Ident,
    pub field: Ident,
    pub type_: Ident,
}
