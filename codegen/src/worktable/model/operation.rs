use proc_macro2::Ident;

#[derive(Debug, Clone)]
pub struct Operation {
    pub name: Ident,
    pub columns: Vec<Ident>,
    pub by: Ident
}