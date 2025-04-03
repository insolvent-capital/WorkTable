use std::collections::HashMap;

use crate::worktable::model::index::Index;
use crate::worktable::model::GeneratorType;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::spanned::Spanned;

fn is_float(ident: &Ident) -> bool {
    matches!(ident.to_string().as_str(), "f64" | "f32")
}

fn is_sized(ident: &Ident) -> bool {
    !matches!(ident.to_string().as_str(), "String")
}

#[derive(Debug, Clone)]
pub struct Columns {
    pub is_sized: bool,
    pub columns_map: HashMap<Ident, TokenStream>,
    pub indexes: HashMap<Ident, Index>,
    pub primary_keys: Vec<Ident>,
    pub generator_type: GeneratorType,
}

#[derive(Debug)]
pub struct Row {
    pub name: Ident,
    pub type_: Ident,
    pub is_primary_key: bool,
    pub gen_type: GeneratorType,
    pub optional: bool,
}

impl Columns {
    pub fn try_from_rows(rows: Vec<Row>, input: &TokenStream) -> syn::Result<Self> {
        let mut columns_map = HashMap::new();
        let mut sized = true;
        let mut pk = vec![];
        let mut gen_type = None;

        for row in rows {
            let type_ = &row.type_;
            if sized {
                sized = is_sized(type_)
            }
            let type_ = if is_float(type_) {
                quote! { ordered_float::OrderedFloat<#type_> }
            } else {
                quote! { #type_ }
            };
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
                pk.push(row.name);
            }
        }

        if pk.is_empty() {
            return Err(syn::Error::new(input.span(), "Primary key must be set"));
        }

        Ok(Self {
            is_sized: sized,
            columns_map,
            indexes: Default::default(),
            primary_keys: pk,
            generator_type: gen_type.expect("set"),
        })
    }
}
