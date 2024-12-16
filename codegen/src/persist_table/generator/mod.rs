use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span};
use syn::ItemStruct;

use crate::name_generator::WorktableNameGenerator;

mod size_measurable;
mod space_deserialize;
mod space_serialize;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub index_type_ident: Ident
}

impl WorktableNameGenerator {
    pub fn get_space_ident(&self) -> Ident {
        Ident::new(format!("{}Space", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_filename(&self) -> String {
        self.name.from_case(Case::Pascal).to_case(Case::Snake)
    }
}
