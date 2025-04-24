use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span};
use syn::ItemStruct;

use crate::name_generator::WorktableNameGenerator;

pub use space_file::WT_INDEX_EXTENSION;

mod space;
mod space_file;

pub struct PersistTableAttributes {
    pub pk_unsized: bool,
}

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub attributes: PersistTableAttributes,
}

impl WorktableNameGenerator {
    pub fn get_space_file_ident(&self) -> Ident {
        Ident::new(
            format!("{}SpaceFile", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_dir_name(&self) -> String {
        self.name.from_case(Case::Pascal).to_case(Case::Snake)
    }

    pub fn get_persistence_engine_ident(&self) -> Ident {
        Ident::new(
            format!("{}PersistenceEngine", self.name).as_str(),
            Span::mixed_site(),
        )
    }

    pub fn get_persistence_task_ident(&self) -> Ident {
        Ident::new(
            format!("{}PersistenceTask", self.name).as_str(),
            Span::mixed_site(),
        )
    }
}
