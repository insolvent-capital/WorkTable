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
                    Self::gen_unique_index_fn(i, idx, &self.columns.columns_map, row_ident.clone())
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
            .get(i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                let link = self.0.indexes.#field_ident.get(&by).map(|kv| kv.get().value)?;
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
            .get(i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> core::result::Result<SelectResult<#row_ident, Self>, WorkTableError> {
                let rows = {
                    self.0.indexes.#field_ident.get(&by)
                        .map(|kv| *kv.1)
                        .collect::<Vec<_>>()
                }.iter().map(|link| {
                    self.0.data.select(*link).map_err(WorkTableError::PagesError)
                })
                .collect::<Result<Vec<_>, _>>()?;
                core::result::Result::Ok(SelectResult::<#row_ident, Self>::new(rows))
            }
        })
    }

    pub fn gen_select_where_fns(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_ident = name_generator.get_row_type_ident();

        let fn_defs = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let type_ = self
                    .columns
                    .columns_map
                    .get(i)
                    .ok_or(syn::Error::new(i.span(), "Row not found"))?;
                if type_.to_string() == "String" {
                    return Ok(quote! {});
                }
                if idx.is_unique {
                    Self::gen_unique_select_where_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        row_ident.clone(),
                    )
                } else {
                    Self::gen_non_unique_select_where_fn(
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

    fn gen_unique_select_where_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_where_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, range: impl std::ops::RangeBounds<#type_>) -> Option<Vec<#row_ident>> {

                let start = match range.start_bound() {
                    std::ops::Bound::Included(val) => *val,
                    std::ops::Bound::Excluded(val) => *val + 1,
                    std::ops::Bound::Unbounded => #type_::MIN,
                };

                let end = match range.end_bound() {
                    std::ops::Bound::Included(val) => *val,
                    std::ops::Bound::Excluded(val) => *val - 1,
                    std::ops::Bound::Unbounded => #type_::MAX,
                };

                let rows = self.0.indexes.#field_ident
                     .range::<#type_, _>((std::ops::Bound::Included(&start), std::ops::Bound::Included(&end)))
                     .map(|(_key, link)| self.0.data.select(*link))
                     .collect::<Result<Vec<_>, _>>()
                     .ok()?;

               if !rows.is_empty() {
                   Some(rows)
               } else {
                   None
               }
            }
        })
    }

    fn gen_non_unique_select_where_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_where_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, range: impl std::ops::RangeBounds<#type_>)  -> core::result::Result<SelectResult<#row_ident, Self>, WorkTableError>  {

                let start = match range.start_bound() {
                    std::ops::Bound::Included(val) => *val,
                    std::ops::Bound::Excluded(val) => *val + 1,
                    std::ops::Bound::Unbounded => #type_::MIN,
                };

                let end = match range.end_bound() {
                    std::ops::Bound::Included(val) => *val,
                    std::ops::Bound::Excluded(val) => *val - 1,
                    std::ops::Bound::Unbounded => #type_::MAX,
                };

                let rows = self.0.indexes.#field_ident
                    .range((std::ops::Bound::Included(&start), std::ops::Bound::Included(&end)))
                    .map(|(_key, link)| { self.0.data.select(*link).map_err(WorkTableError::PagesError)
                        }).collect::<Result<Vec<_>, _>>()?;

                core::result::Result::Ok(SelectResult::<#row_ident, Self>::new(rows))

            }
        })
    }
}
