use proc_macro2::Ident;

#[derive(Debug, Default)]
pub struct Config {
    pub page_size: Option<u32>,
    pub row_derives: Vec<Ident>,
}
