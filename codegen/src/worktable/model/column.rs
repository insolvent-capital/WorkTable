use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use syn::spanned::Spanned;

use crate::worktable::model::index::Index;

#[derive(Debug, Clone)]
pub struct Columns {
    pub columns_map: HashMap<Ident, Ident>,
    pub indexes: HashMap<Ident, Index>,
    pub primary_key: Ident,
}

#[derive(Debug)]
pub struct Row {
    pub name: Ident,
    pub type_: Ident,
    pub is_primary_key: bool,
}

impl Columns {
    pub fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = HashMap::new();
        let mut pk = None;

        for row in rows {
            columns_map.insert(row.name.clone(), row.type_.clone());

            if row.is_primary_key {
                if let Some(_) = pk {
                    return Err(syn::Error::new(
                        input.span(),
                        "Only one primary key column allowed",
                    ));
                } else {
                    pk = Some(row.name)
                }
            }
        }

        if pk.is_none() {
            return Err(syn::Error::new(input.span(), "Primary key must be set"));
        }

        Ok(Self {
            columns_map,
            indexes: Default::default(),
            primary_key: pk.unwrap(),
        })
    }
}
