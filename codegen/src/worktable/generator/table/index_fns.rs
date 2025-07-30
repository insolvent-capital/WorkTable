use std::collections::HashMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::{is_float, WorktableNameGenerator};
use crate::worktable::generator::Generator;
use crate::worktable::model::Index;

impl Generator {
    pub fn gen_table_index_fns(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_ident = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

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
                        &column_range_type,
                        &row_fields_ident,
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
        let by = if is_float(type_.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                let link = self.0.indexes.#field_ident.get(#by).map(|kv| kv.get().value)?;
                self.0.data.select_non_ghosted(link).ok()
            }
        })
    }

    fn gen_non_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
        column_range_type: &Ident,
        row_fields_ident: &Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;
        let row_field_ident = &idx.field;
        let by = if is_float(type_.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> SelectQueryBuilder<#row_ident,
                                                                     impl DoubleEndedIterator<Item = #row_ident> + '_,
                                                                     #column_range_type,
                                                                     #row_fields_ident>
            {
                let rows = self.0.indexes.#field_ident
                    .get(#by)
                    .into_iter()
                    .filter_map(|(_, link)| self.0.data.select_non_ghosted(*link).ok())
                    .filter(move |r| &r.#row_field_ident == &by);

                SelectQueryBuilder::new(rows)
            }
        })
    }
}
