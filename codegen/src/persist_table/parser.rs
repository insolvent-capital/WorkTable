use crate::persist_table::generator::PersistTableAttributes;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use syn::spanned::Spanned;
use syn::{Attribute, ItemStruct};

pub struct Parser;

impl Parser {
    pub fn parse_struct(input: TokenStream) -> syn::Result<ItemStruct> {
        match syn::parse2::<ItemStruct>(input.clone()) {
            Ok(data) => Ok(data),
            Err(err) => Err(syn::Error::new(input.span(), err.to_string())),
        }
    }

    pub fn parse_pk_ident(item: &ItemStruct) -> Ident {
        // WorkTable<#row_type, #pk_type, <#pk_type as TablePrimaryKey>::Generator, #const_name>
        let type_str = item
            .fields
            .iter()
            .next()
            .unwrap()
            .ty
            .to_token_stream()
            .to_string();
        let mut split = type_str.split("<");
        split.next();
        let mut gens = split.next().unwrap().split(",");
        let pk_type = gens.nth(1).unwrap();

        Ident::new(pk_type.trim(), Span::mixed_site())
    }

    pub fn parse_attributes(attrs: &Vec<Attribute>) -> PersistTableAttributes {
        let mut res = PersistTableAttributes { pk_unsized: false };

        for attr in attrs {
            if attr.path().to_token_stream().to_string().as_str() == "table" {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("pk_unsized") {
                        res.pk_unsized = true;
                        return Ok(());
                    }
                    Ok(())
                })
                .expect("always ok even on unrecognized attrs");
            }
        }

        res
    }
}
