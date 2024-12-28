use std::str::FromStr;

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span};
use syn::ItemStruct;

use crate::name_generator::WorktableNameGenerator;

pub use space_file::{WT_DATA_EXTENSION, WT_INDEX_EXTENSION, WT_INFO_EXTENSION};

mod size_measurable;
mod space_file;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub index_type_ident: Ident,
}

impl WorktableNameGenerator {
    pub fn get_space_file_ident(&self) -> Ident {
        Ident::new(
            format!("{}SpaceFile", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_space_ident(&self) -> Ident {
        Ident::new(format!("{}Space", self.name).as_str(), Span::mixed_site())
    }

    pub fn get_dir_name(&self) -> String {
        self.name.from_case(Case::Pascal).to_case(Case::Snake)
    }
}
