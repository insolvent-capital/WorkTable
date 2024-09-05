use std::collections::HashMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;
use crate::worktable::model::Index;

impl Generator {
    /// Generates type alias for new [`WorkTable`].
    ///
    /// [`WorkTable`]: worktable::WorkTable
    pub fn gen_table_def(&mut self) -> TokenStream {
        let name = &self.name;
        let ident = Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site());
        self.table_name = Some(ident.clone());

        let row_type = self.row_name.as_ref().unwrap();
        let pk_type = &self.pk.as_ref().unwrap().ident;
        let index_type = self.index_name.as_ref().unwrap();

        quote! {
            #[derive(Debug, Default)]
            pub struct #ident(WorkTable<#row_type, #pk_type, #index_type>);

            impl #ident {
                pub fn select(&self, pk: #pk_type) -> Option<#row_type> {
                    self.0.select(pk)
                }

                pub fn insert<const ROW_SIZE_HINT: usize>(&self, row: #row_type) -> core::result::Result<#pk_type, WorkTableError> {
                    self.0.insert::<ROW_SIZE_HINT>(row)
                }

                pub fn update<const ROW_SIZE_HINT: usize>(&self, row: #row_type) -> core::result::Result<(), WorkTableError> {
                    self.0.update::<ROW_SIZE_HINT>(row)
                }

                pub fn get_next_pk(&self) -> #pk_type {
                    self.0.get_next_pk()
                }
            }
        }
    }

    pub fn gen_table_index_impl(&mut self) -> syn::Result<TokenStream> {
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
                        self.row_name.clone().unwrap(),
                    )
                } else {
                    Self::gen_non_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        self.row_name.clone().unwrap(),
                    )
                }
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;

        let table_ident = self.table_name.clone().unwrap();
        Ok(quote! {
            impl #table_ident {
                #(#fn_defs)*
            }
        })
    }

    fn gen_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, Ident>,
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
                let link = self.0.indexes.#field_ident.peek(&by, &guard)?;
                self.0.data.select(*link).ok()
            }
        })
    }

    fn gen_non_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, Ident>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(&i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> core::result::Result<Vec<#row_ident>, WorkTableError> {
                {
                    let guard = Guard::new();
                    self.0.indexes.#field_ident
                        .peek(&by, &guard)
                        .ok_or(WorkTableError::NotFound)?
                        .iter()
                        .map(|l| *l.as_ref())
                        .collect::<Vec<_>>()
                }.iter().map(|link| {
                    self.0.data.select(*link).map_err(WorkTableError::PagesError)
                })
                .collect()
            }
        })
    }
}
