use std::collections::HashMap;

use crate::worktable::model::index::Index;
use crate::worktable::model::GeneratorType;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::spanned::Spanned;

#[derive(Debug, Clone)]
pub struct Columns {
    pub columns_map: HashMap<Ident, TokenStream>,
    pub indexes: HashMap<Ident, Index>,
    pub primary_keys: (Vec<Ident>, Ident),
    pub generator_type: GeneratorType,
}

#[derive(Debug)]
pub struct Row {
    pub name: Ident,
    pub type_: Ident,
    pub is_primary_key: bool,
    pub gen_type: GeneratorType,
    pub index_type: Ident,
    pub optional: bool,
}

impl Columns {
    pub fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = HashMap::new();
        let mut pk = vec![];
        let mut gen_type = None;
        let mut index_type = None;

        for row in rows {
            let type_ = &row.type_;
            let type_ = if row.optional {
                quote! { core::option::Option<#type_> }
            } else {
                quote! { #type_ }
            };
            columns_map.insert(row.name.clone(), type_);

            if row.is_primary_key {
                if let Some(t) = gen_type {
                    if t != row.gen_type {
                        return Err(syn::Error::new(input.span(), "Generator type must be same"));
                    }
                } else {
                    gen_type = Some(row.gen_type)
                }
                if let Some(t) = index_type.as_ref() {
                    if t != &row.index_type {
                        return Err(syn::Error::new(input.span(), "Index type must be same"));
                    }
                } else {
                    index_type = Some(row.index_type)
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
            primary_keys: (pk, index_type.unwrap()),
            generator_type: gen_type.expect("set"),
        })
    }
}
