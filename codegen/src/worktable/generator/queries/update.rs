use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use std::collections::HashMap;
use std::fmt::format;

use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;

impl Generator {
    pub fn gen_query_update_impl(&mut self) -> syn::Result<TokenStream> {
        if let Some(q) = &self.queries {
            let custom_updates = self.gen_custom_updates(q.updates.clone());

            let table_ident = self.table_name.as_ref().unwrap();
            Ok(quote! {
                impl #table_ident {
                    #custom_updates
                }
            })
        } else {
            Ok(quote! {})
        }
    }

    fn gen_custom_updates(&mut self, updates: HashMap<Ident, Operation>) -> TokenStream {
        let defs = updates.iter().map(|(name, op)| {
            let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);
            let index = self.columns.indexes.values().find(|idx| {
                idx.field.to_string() == op.by.to_string()
            });

            let idents = &op.columns;
            if let Some(index) = index {
                let index_name = &index.name;

                if index.is_unique {
                    Self::gen_unique_update(snake_case_name, name, index_name, idents)
                } else {
                    Self::gen_non_unique_update(snake_case_name, name, index_name, idents)
                }
            } else {
                if self.columns.primary_key.to_string() == op.by.to_string() {
                    Self::gen_pk_update(snake_case_name, name, idents)
                } else {
                    todo!()
                }
            }
        }).collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_pk_update(snake_case_name: String, name: &Ident, idents: &Vec<Ident>) -> TokenStream {
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let check_ident = Ident::new(format!("check_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let lock_ident = Ident::new(format!("lock_{snake_case_name}").as_str(), Span::mixed_site());
        let unlock_ident = Ident::new(format!("unlock_{snake_case_name}").as_str(), Span::mixed_site());
        let verify_ident = Ident::new(format!("verify_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let row_updates = idents.iter().map(|i| {
            quote! {
                archived.inner.#i = row.#i;
            }
        }).collect::<Vec<_>>();

        quote! {
                pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                    let op_id = self.0.lock_map.next_id();
                    let lock = std::sync::Arc::new(Lock::new(op_id.into()));

                    self.0.lock_map.insert(op_id.into(), lock.clone());

                    let guard = Guard::new();
                    let link = self.0.pk_map.peek(&by, &guard).ok_or(WorkTableError::NotFound)?;
                    let id = self.0.data.with_ref(*link, |archived| {
                        archived.#check_ident()
                    }).map_err(WorkTableError::PagesError)?;
                    if let Some(id) = id {
                        if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                            lock.as_ref().await
                        }
                    }
                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        while !archived.#verify_ident(op_id) {
                            unsafe {
                                archived.#lock_ident(op_id)
                            }
                        }
                    }).map_err(WorkTableError::PagesError)? };

                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)? };

                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        unsafe {
                            archived.#unlock_ident()
                        }
                    }).map_err(WorkTableError::PagesError)? };
                    lock.unlock();

                    Ok(())
                }
            }
    }

    fn gen_non_unique_update(snake_case_name: String, name: &Ident, index: &Ident, idents: &Vec<Ident>) -> TokenStream {
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let check_ident = Ident::new(format!("check_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let lock_ident = Ident::new(format!("lock_{snake_case_name}").as_str(), Span::mixed_site());
        let unlock_ident = Ident::new(format!("unlock_{snake_case_name}").as_str(), Span::mixed_site());
        let verify_ident = Ident::new(format!("verify_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let row_updates = idents.iter().map(|i| {
            quote! {
                archived.inner.#i = row.#i;
            }
        }).collect::<Vec<_>>();

        quote! {
                pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                    let op_id = self.0.lock_map.next_id();
                    let lock = std::sync::Arc::new(Lock::new(op_id.into()));

                    self.0.lock_map.insert(op_id.into(), lock.clone());

                    let guard = Guard::new();

                    let rows_to_update = self.0.indexes.#index.peek(&by, &guard).ok_or(WorkTableError::NotFound)?;
                    for link in rows_to_update.iter() {
                        let id = self.0.data.with_ref(*link.as_ref(), |archived| {
                            archived.#check_ident()
                        }).map_err(WorkTableError::PagesError)?;
                        if let Some(id) = id {
                            if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                                lock.as_ref().await
                            }
                        }
                        unsafe { self.0.data.with_mut_ref(*link.as_ref(), |archived| {
                            while !archived.#verify_ident(op_id) {
                                unsafe {
                                    archived.#lock_ident(op_id)
                                }
                            }
                        }).map_err(WorkTableError::PagesError)? };
                    }

                    for link in rows_to_update.iter() {
                        unsafe { self.0.data.with_mut_ref(*link.as_ref(), |archived| {
                            #(#row_updates)*
                        }).map_err(WorkTableError::PagesError)? };
                    }

                    for link in rows_to_update.iter() {
                        unsafe { self.0.data.with_mut_ref(*link.as_ref(), |archived| {
                            unsafe {
                                archived.#unlock_ident()
                            }
                        }).map_err(WorkTableError::PagesError)? };
                    }
                    lock.unlock();

                    Ok(())
                }
            }
    }

    fn gen_unique_update(snake_case_name: String, name: &Ident, index: &Ident, idents: &Vec<Ident>) -> TokenStream {
        let method_ident = Ident::new(format!("update_{snake_case_name}").as_str(), Span::mixed_site());

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let check_ident = Ident::new(format!("check_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let lock_ident = Ident::new(format!("lock_{snake_case_name}").as_str(), Span::mixed_site());
        let unlock_ident = Ident::new(format!("unlock_{snake_case_name}").as_str(), Span::mixed_site());
        let verify_ident = Ident::new(format!("verify_{snake_case_name}_lock").as_str(), Span::mixed_site());
        let row_updates = idents.iter().map(|i| {
            quote! {
                archived.inner.#i = row.#i;
            }
        }).collect::<Vec<_>>();

        quote! {
                pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                    let op_id = self.0.lock_map.next_id();
                    let lock = std::sync::Arc::new(Lock::new(op_id.into()));

                    self.0.lock_map.insert(op_id.into(), lock.clone());

                    let guard = Guard::new();
                    let link = self.0.indexes.#index.peek(&by, &guard).ok_or(WorkTableError::NotFound)?;
                    let id = self.0.data.with_ref(*link, |archived| {
                        archived.#check_ident()
                    }).map_err(WorkTableError::PagesError)?;
                    if let Some(id) = id {
                        if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                            lock.as_ref().await
                        }
                    }
                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        while !archived.#verify_ident(op_id) {
                            unsafe {
                                archived.#lock_ident(op_id)
                            }
                        }
                    }).map_err(WorkTableError::PagesError)? };

                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)? };

                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        unsafe {
                            archived.#unlock_ident()
                        }
                    }).map_err(WorkTableError::PagesError)? };
                    lock.unlock();

                    Ok(())
                }
            }
    }
}
