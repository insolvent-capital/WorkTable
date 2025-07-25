use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use std::collections::HashMap;

impl Generator {
    pub fn gen_query_in_place_impl(&self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        let custom_in_place = if let Some(q) = &self.queries {
            let custom_in_place = self.gen_in_place_queries(q.in_place.clone());
            quote! {
                #custom_in_place
            }
        } else {
            quote! {}
        };

        Ok(quote! {
            impl #table_ident {
                #custom_in_place
            }
        })
    }

    fn gen_in_place_queries(&self, in_place_queries: HashMap<Ident, Operation>) -> TokenStream {
        let defs = in_place_queries
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);
                let by_type = self.columns.columns_map.get(&op.by).unwrap();
                if let Some(index) = index {
                    let _index_name = &index.name;

                    if index.is_unique {
                        todo!()
                    } else {
                        todo!()
                    }
                } else if self.columns.primary_keys.len() == 1 {
                    self.gen_primary_key_in_place(snake_case_name, by_type, &op.columns)
                } else {
                    todo!()
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_primary_key_in_place(
        &self,
        snake_case_name: String,
        by_type: &TokenStream,
        columns: &[Ident],
    ) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let pk_type = name_generator.get_primary_key_type_ident();
        let lock_ident =
            WorktableNameGenerator::get_update_in_place_query_lock_ident(&snake_case_name);

        let method_ident = Ident::new(
            format!("update_{snake_case_name}_in_place").as_str(),
            Span::mixed_site(),
        );

        let types = columns
            .iter()
            .map(|c| self.columns.columns_map.get(c).unwrap())
            .collect::<Vec<_>>();
        let column_types = if types.len() == 1 {
            let t = types[0];
            quote! {
                &mut <#t as rkyv::Archive>::Archived
            }
        } else {
            let types = types.iter().map(|t| {
                quote! {
                    &mut <#t as rkyv::Archive>::Archived
                }
            });
            quote! {
                ( #(#types),* )
            }
        };
        let column_fields = if columns.len() == 1 {
            let i = &columns[0];
            quote! {
                &mut archived.inner.#i
            }
        } else {
            let columns = columns.iter().map(|i| {
                quote! {
                    &mut archived.inner.#i
                }
            });
            quote! {
                ( #(#columns),* )
            }
        };
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        quote! {
            pub async fn #method_ident<F: FnMut(#column_types)>(
                &self,
                mut f: F,
                by: #by_type,
            ) -> eyre::Result<()> {
                let pk: #pk_type = by.into();
                let link = self
                    .0
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value)
                    .ok_or(WorkTableError::NotFound)?;
                let lock = {
                    #custom_lock
                };
                unsafe {
                    self.0
                        .data
                        .with_mut_ref(link, move |archived| f(#column_fields))
                        .map_err(WorkTableError::PagesError)?
                    };

                lock.unlock();
                self.0.lock_map.remove_with_lock_check(&pk).await;

                Ok(())
            }
        }
    }
}
