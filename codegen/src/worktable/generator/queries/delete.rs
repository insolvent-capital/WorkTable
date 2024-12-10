use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use std::collections::HashMap;

impl Generator {
    pub fn gen_query_delete_impl(&mut self) -> syn::Result<TokenStream> {
        let custom_deletes = if let Some(q) = &self.queries {
            let custom_deletes = self.gen_custom_deletes(q.deletes.clone());

            quote! {
                #custom_deletes
            }
        } else {
            quote! {}
        };
        let full_row_delete = self.gen_full_row_delete();

        let table_ident = self.table_name.as_ref().unwrap();
        Ok(quote! {
            impl #table_ident {
                #full_row_delete
                #custom_deletes
            }
        })
    }

    fn gen_full_row_delete(&mut self) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;

        quote! {
            pub async fn delete(&self, pk: #pk_ident) -> core::result::Result<(), WorkTableError> {
                let link = {
                    let guard = Guard::new();
                    TableIndex::peek(&self.0.pk_map, &pk).ok_or(WorkTableError::NotFound)?
                };
                let id = self.0.data.with_ref(link, |archived| {
                    archived.is_locked()
                }).map_err(WorkTableError::PagesError)?;
                if let Some(id) = id {
                    if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                        lock.as_ref().await
                    }
                }
                let row = self.select(pk.clone()).unwrap();
                self.0.indexes.delete_row(row, link)?;
                self.0.pk_map.remove(&pk);
                self.0.data.delete(link).map_err(WorkTableError::PagesError)?;

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_custom_deletes(&mut self, deleted: HashMap<Ident, Operation>) -> TokenStream {
        let defs = deleted
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);
                let method_ident = Ident::new(
                    format!("delete_{snake_case_name}").as_str(),
                    Span::mixed_site(),
                );
                let index = self
                    .columns
                    .indexes
                    .values()
                    .find(|idx| idx.field.to_string() == op.by.to_string());
                let type_ = self.columns.columns_map.get(&op.by).unwrap();
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        Self::gen_unique_delete(&type_, &method_ident, index_name)
                    } else {
                        Self::gen_non_unique_delete(&type_, &method_ident, index_name)
                    }
                } else {
                    Self::gen_brute_force_delete_field(&op.by, &type_, &method_ident)
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_brute_force_delete_field(
        field: &Ident,
        type_: &TokenStream,
        name: &Ident,
    ) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                self.iter_with_async(|row| {
                    if row.#field == by {
                        futures::future::Either::Left(async move {
                            self.delete(row.id.into()).await
                        })
                    } else {
                        futures::future::Either::Right(async {
                            Ok(())
                        })
                    }
                }).await?;
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_non_unique_delete(type_: &TokenStream, name: &Ident, index: &Ident) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let rows_to_update = TableIndex::peek(&self.0.indexes.#index, &by);
                if let Some(rows) = rows_to_update {
                    for link in rows.iter() {
                        let row = self.0.data.select(*link.as_ref()).map_err(WorkTableError::PagesError)?;
                        self.delete(row.id.into()).await?;
                    }
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_delete(type_: &TokenStream, name: &Ident, index: &Ident) -> TokenStream {
        quote! {
            pub async fn #name(&self, by: #type_) -> core::result::Result<(), WorkTableError> {
                let row_to_update = TableIndex::peek(&self.0.indexes.#index, &by);
                if let Some(link) = row_to_update {
                    let row = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                    self.delete(row.id.into()).await?;
                }
                core::result::Result::Ok(())
            }
        }
    }
}
