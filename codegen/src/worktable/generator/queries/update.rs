use proc_macro2::Literal;
use std::collections::HashMap;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use crate::worktable::model::Operation;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_query_update_impl(&mut self) -> syn::Result<TokenStream> {
        let custom_updates = if let Some(q) = &self.queries {
            let custom_updates = self.gen_custom_updates(q.updates.clone());

            quote! {
                #custom_updates
            }
        } else {
            quote! {}
        };
        let full_row_update = self.gen_full_row_update();

        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();
        Ok(quote! {
            impl #table_ident {
                #full_row_update
                #custom_updates
            }
        })
    }

    fn gen_full_row_update(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let avt_type_ident = name_generator.get_available_type_ident();

        let row_updates = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let idents: Vec<_> = self
            .columns
            .indexes
            .values()
            .map(|idx| idx.field.clone())
            .collect();
        let diff_container = quote! {
            let row_old = self.select(pk.clone()).unwrap();
            let row_new = row.clone();
            let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
        };
        let diff_process = self.gen_process_diffs_on_index(idents.as_slice(), idents.as_slice());

        quote! {
            pub async fn update(&self, row: #row_ident) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                let op_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(Lock::new());
                self.0.lock_map.insert(op_id.into(), lock.clone());

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#row_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                let link = self.0
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                #diff_container
                #diff_process

                let id = self.0.data.with_ref(link, |archived| {
                    archived.is_locked()
                }).map_err(WorkTableError::PagesError)?;
                if let Some(id) = id {
                    if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                        lock.as_ref().await
                    }
                }
                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    archived.lock = op_id.into();
                }).map_err(WorkTableError::PagesError)? };
                unsafe { self.0.data.with_mut_ref(link, move |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };
                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    unsafe {
                        archived.lock = 0u16.into();
                    }
                }).map_err(WorkTableError::PagesError)? };
                lock.unlock();
                self.0.lock_map.remove(&op_id.into());
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_custom_updates(&mut self, updates: HashMap<Ident, Operation>) -> TokenStream {
        let defs = updates
            .iter()
            .map(|(name, op)| {
                let snake_case_name = name
                    .to_string()
                    .from_case(Case::Pascal)
                    .to_case(Case::Snake);
                let index = self.columns.indexes.values().find(|idx| idx.field == op.by);

                let indexes_columns: Option<Vec<_>> = {
                    let columns: Vec<_> = self
                        .columns
                        .indexes
                        .values()
                        .filter(|idx| op.columns.contains(&idx.field))
                        .map(|idx| idx.field.clone())
                        .collect();

                    if columns.is_empty() {
                        None
                    } else {
                        Some(columns)
                    }
                };

                let idents = &op.columns;
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        self.gen_unique_update(snake_case_name, name, index_name, idents)
                    } else {
                        self.gen_non_unique_update(snake_case_name, name, index_name, idents)
                    }
                } else if self.columns.primary_keys.len() == 1 {
                    if *self.columns.primary_keys.first().unwrap() == op.by {
                        self.gen_pk_update(snake_case_name, name, idents, indexes_columns.as_ref())
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#defs)*
        }
    }

    fn gen_process_diffs_on_index(&self, idents: &[Ident], idx_idents: &[Ident]) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let diff = idents
            .iter()
            .filter(|i| idx_idents.contains(i))
            .map(|i| {
                let diff_key = Literal::string(i.to_string().as_str());
                quote! {
                    let old = &row_old.#i;
                    let new = &row_new.#i;

                    if old != new {
                        let diff = Difference::<#avt_type_ident> {
                            old: old.clone().into(),
                            new: new.clone().into(),
                        };

                        diffs.insert(#diff_key, diff);
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #(#diff)*
            self.0.indexes.process_difference(link, diffs)?;
        }
    }

    fn gen_pk_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
    ) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());

        let check_ident = Ident::new(
            format!("check_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let verify_ident = Ident::new(
            format!("verify_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let diff_process = if let Some(idx_idents) = idx_idents {
            let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
            let avt_type_ident = name_generator.get_available_type_ident();
            let diff_container = quote! {
                let row_old = self.select(by.clone()).unwrap();
                let row_new = row.clone();
                let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
            };

            let process = self.gen_process_diffs_on_index(idents, idx_idents.as_slice());
            quote! {
                #diff_container
                #process
            }
        } else {
            quote! {}
        };

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #pk_ident) -> core::result::Result<(), WorkTableError> {
                let op_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(Lock::new());

                self.0.lock_map.insert(op_id.into(), lock.clone());

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                let link = self.0
                        .pk_map
                        .get(&by)
                        .map(|v| v.get().value)
                        .ok_or(WorkTableError::NotFound)?;

                #diff_process

                let id = self.0.data.with_ref(link, |archived| {
                    archived.#check_ident()
                }).map_err(WorkTableError::PagesError)?;
                if let Some(id) = id {
                    if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                        lock.as_ref().await
                    }
                }
                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    while !archived.#verify_ident(op_id) {
                        unsafe {
                            archived.#lock_ident(op_id)
                        }
                    }
                }).map_err(WorkTableError::PagesError)? };

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    unsafe {
                        archived.#unlock_ident()
                    }
                }).map_err(WorkTableError::PagesError)? };
                lock.unlock();
                self.0.lock_map.remove(&op_id.into());

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_non_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Ident,
        idents: &[Ident],
    ) -> TokenStream {
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let check_ident = Ident::new(
            format!("check_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let verify_ident = Ident::new(
            format!("verify_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );
        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut row.#i);
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                let op_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(Lock::new());

                self.0.lock_map.insert(op_id.into(), lock.clone());

                for (_, link) in self.0.indexes.#index.get(&by) {
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
                }

                for (_, link) in self.0.indexes.#index.get(&by) {
                    let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                    let mut row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)? };
                }

                for (_, link) in self.0.indexes.#index.get(&by) {
                    unsafe { self.0.data.with_mut_ref(*link, |archived| {
                        unsafe {
                            archived.#unlock_ident()
                        }
                    }).map_err(WorkTableError::PagesError)? };
                }
                lock.unlock();
                self.0.lock_map.remove(&op_id.into());

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_unique_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        index: &Ident,
        idents: &[Ident],
    ) -> TokenStream {
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());

        let check_ident = Ident::new(
            format!("check_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );
        let lock_ident = Ident::new(
            format!("lock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let unlock_ident = Ident::new(
            format!("unlock_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let verify_ident = Ident::new(
            format!("verify_{snake_case_name}_lock").as_str(),
            Span::mixed_site(),
        );
        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut row.#i);
                }
            })
            .collect::<Vec<_>>();

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                let op_id = self.0.lock_map.next_id();
                let lock = std::sync::Arc::new(Lock::new());

                self.0.lock_map.insert(op_id.into(), lock.clone());

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };
                let link = self.0.indexes.#index.get(&by).map(|kv| kv.get().value).ok_or(WorkTableError::NotFound)?;
                let id = self.0.data.with_ref(link, |archived| {
                    archived.#check_ident()
                }).map_err(WorkTableError::PagesError)?;
                if let Some(id) = id {
                    if let Some(lock) = self.0.lock_map.get(&(id.into())) {
                        lock.as_ref().await
                    }
                }
                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    while !archived.#verify_ident(op_id) {
                        unsafe {
                            archived.#lock_ident(op_id)
                        }
                    }
                }).map_err(WorkTableError::PagesError)? };

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    unsafe {
                        archived.#unlock_ident()
                    }
                }).map_err(WorkTableError::PagesError)? };
                lock.unlock();
                self.0.lock_map.remove(&op_id.into());

                core::result::Result::Ok(())
            }
        }
    }
}
