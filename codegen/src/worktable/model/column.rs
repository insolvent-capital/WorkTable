use std::cmp::PartialEq;
use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use syn::spanned::Spanned;
use crate::worktable::model::GeneratorType;
use crate::worktable::model::index::Index;

#[derive(Debug, Clone)]
pub struct Columns {
    pub columns_map: HashMap<Ident, Ident>,
    pub indexes: HashMap<Ident, Index>,
    pub primary_keys: Vec<Ident>,
    pub generator_type: GeneratorType
}

#[derive(Debug)]
pub struct Row {
    pub name: Ident,
    pub type_: Ident,
    pub is_primary_key: bool,
    pub gen_type: GeneratorType,
}

impl Columns {
    pub fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = HashMap::new();
        let mut pk = vec![];
        let mut gen_type = None;

        for row in rows {
            columns_map.insert(row.name.clone(), row.type_.clone());

            if row.is_primary_key {
                if let Some(t) = gen_type {
                    if t != row.gen_type {
                        return Err(syn::Error::new(input.span(), "Generator type must be same"));
                    }
                } else {
                    gen_type = Some(row.gen_type)
                }
                pk.push(row.name);
            }
        }

        if pk.is_empty() {
            return Err(syn::Error::new(input.span(), "Primary key must be set"));
        }

        Ok(Self {
            columns_map,
            indexes: Default::default(),
            primary_keys: pk,
            generator_type: gen_type.expect("set")
        })
    }
}
