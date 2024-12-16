use std::collections::HashMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Index;

impl Generator {
    pub fn gen_table_index_fns(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_ident = name_generator.get_row_type_ident();

        let fn_defs = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                if idx.is_unique {
                    Self::gen_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        row_ident.clone(),
                    )
                } else {
                    Self::gen_non_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        row_ident.clone(),
                    )
                }
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;

        Ok(quote! {
            impl #ident {
                #(#fn_defs)*
            }
        })
    }

    fn gen_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(&i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                let guard = Guard::new();
                let link = TableIndex::peek(&self.0.indexes.#field_ident, &by)?;
                self.0.data.select(link).ok()
            }
        })
    }

    fn gen_non_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(&i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> core::result::Result<SelectResult<#row_ident, Self>, WorkTableError> {
                let rows = {
                    TableIndex::peek(&self.0.indexes.#field_ident, &by)
                        .ok_or(WorkTableError::NotFound)?
                        .iter()
                        .map(|l| *l.as_ref())
                        .collect::<Vec<_>>()
                }.iter().map(|link| {
                    self.0.data.select(*link).map_err(WorkTableError::PagesError)
                })
                .collect::<Result<Vec<_>, _>>()?;
                core::result::Result::Ok(SelectResult::<#row_ident, Self>::new(rows))
            }
        })
    }
}