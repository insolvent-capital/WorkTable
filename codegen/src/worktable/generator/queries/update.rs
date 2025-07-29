use proc_macro2::Literal;
use std::collections::HashMap;

use crate::name_generator::{is_float, WorktableNameGenerator};
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

        let diff_process = self.gen_process_diffs_on_index(idents.as_slice(), Some(&idents));
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let full_row_lock = self.gen_full_lock_for_update();
        let size_check = if self.columns.is_sized {
            quote! {}
        } else {
            quote! {
                if bytes.len() >= link.length as usize {
                    lock.unlock();  // Releases locks
                    let lock = {
                       #full_row_lock
                    };
                    let row_old = self.0.data.select(link)?;
                    self.reinsert(row_old, row)?;

                    lock.unlock();
                    self.0.lock_map.remove_with_lock_check(&pk).await; // Removes locks

                    return core::result::Result::Ok(());
                }
            }
        };

        quote! {
            pub async fn update(&self, row: #row_ident) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                let lock = {
                    #full_row_lock
                };

                let link = self.0
                    .pk_map
                    .get(&pk)
                    .map(|v| v.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                #size_check

                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#row_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };

                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #diff_process
                #persist_op

                unsafe { self.0.data.with_mut_ref(link, move |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                lock.unlock();  // Releases locks
                self.0.lock_map.remove_with_lock_check(&pk).await; // Removes locks

                #persist_call

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
                let unsized_columns = if self.columns.is_sized {
                    None
                } else {
                    let fields = op
                        .columns
                        .iter()
                        .filter(|c| {
                            self.columns.columns_map.get(c).unwrap().to_string() == "String"
                        })
                        .collect::<Vec<_>>();
                    if fields.is_empty() {
                        None
                    } else {
                        Some(fields)
                    }
                };

                let idents = &op.columns;
                if let Some(index) = index {
                    let index_name = &index.name;

                    if index.is_unique {
                        self.gen_unique_update(
                            snake_case_name,
                            name,
                            index_name,
                            idents,
                            indexes_columns.as_ref(),
                            unsized_columns,
                        )
                    } else {
                        self.gen_non_unique_update(
                            snake_case_name,
                            name,
                            index_name,
                            idents,
                            indexes_columns.as_ref(),
                            unsized_columns,
                        )
                    }
                } else if self.columns.primary_keys.len() == 1 {
                    if *self.columns.primary_keys.first().unwrap() == op.by {
                        self.gen_pk_update(
                            snake_case_name,
                            name,
                            idents,
                            indexes_columns.as_ref(),
                            unsized_columns,
                        )
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

    fn gen_persist_call(&self) -> TokenStream {
        if self.is_persist {
            quote! {
                if let Operation::Update(op) = &mut op {
                     op.bytes = self.0.data.select_raw(link)?;
                } else {
                    unreachable!("")
                };
                self.2.apply_operation(op);
            }
        } else {
            quote! {}
        }
    }

    fn gen_size_check(&self, unsized_fields: Option<Vec<&Ident>>, idents: &[Ident]) -> TokenStream {
        if let Some(f) = unsized_fields {
            let fields_check: Vec<_> = f
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(format!("get_{f}_size").as_str(), Span::call_site());
                    quote! {
                        need_to_reinsert |= archived_row.#fn_ident() > self.#fn_ident(link)?;
                    }
                })
                .collect();
            let row_updates = idents
                .iter()
                .map(|i| {
                    quote! {
                        row_new.#i = row.#i;
                    }
                })
                .collect::<Vec<_>>();
            let full_row_lock = self.gen_full_lock_for_update();

            quote! {
                let mut need_to_reinsert = false;
                #(#fields_check)*
                if need_to_reinsert {
                    lock.unlock();
                    let lock = {
                        #full_row_lock
                    };

                    let row_old = self.select(pk.clone()).expect("should not be deleted by other thread");
                    let mut row_new = row_old.clone();
                    let pk = row_old.get_primary_key().clone();
                    #(#row_updates)*
                    self.reinsert(row_old, row_new)?;

                    lock.unlock();  // Releases locks
                    self.0.lock_map.remove_with_lock_check(&pk).await; // Removes locks

                    return core::result::Result::Ok(());
                }
            }
        } else {
            quote! {}
        }
    }

    fn gen_persist_op(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let secondary_events_ident = name_generator.get_space_secondary_index_events_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();

        if self.is_persist {
            quote! {
                let mut op: Operation<
                    <<#primary_key_ident as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    #primary_key_ident,
                    #secondary_events_ident
                > = Operation::Update(UpdateOperation {
                    id: op_id,
                    secondary_keys_events,
                    bytes: updated_bytes,
                    link,
                });
            }
        } else {
            quote! {}
        }
    }

    fn gen_process_diffs_on_index(
        &self,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
    ) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let diff_container = if idx_idents.is_some() {
            quote! {
                let row_old = self.0.data.select(link)?;
                let row_new = row.clone();
                let updated_bytes: Vec<u8> = vec![];
                let mut diffs: std::collections::HashMap<&str, Difference<#avt_type_ident>> = std::collections::HashMap::new();
            }
        } else {
            quote! {
                let updated_bytes: Vec<u8> = vec![];
            }
        };

        let diff = if let Some(idx_idents) = idx_idents {
            idents
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
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let process_difference = if self.is_persist {
            if idx_idents.is_some() {
                quote! {
                    let secondary_keys_events = self.0.indexes.process_difference_cdc(link, diffs)?;
                }
            } else {
                quote! {
                    let secondary_keys_events = core::default::Default::default();
                }
            }
        } else if idx_idents.is_some() {
            quote! {
                self.0.indexes.process_difference(link, diffs)?;
            }
        } else {
            quote! {}
        };

        quote! {
            #diff_container
            #(#diff)*
            #process_difference
        }
    }

    fn gen_pk_update(
        &self,
        snake_case_name: String,
        name: &Ident,
        idents: &[Ident],
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let pk_ident = &self.pk.as_ref().unwrap().ident;
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );
        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let size_check = self.gen_size_check(unsized_fields, idents);
        let diff_process = self.gen_process_diffs_on_index(idents, idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, pk: #pk_ident) -> core::result::Result<(), WorkTableError> {
                let lock = {
                    #custom_lock
                };

                let link = self.0
                        .pk_map
                        .get(&pk)
                        .map(|v| v.get().value)
                        .ok_or(WorkTableError::NotFound)?;

                let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row).map_err(|_| WorkTableError::SerializeError)?;
                let mut archived_row = unsafe { rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..]).unseal_unchecked() };

                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #size_check
                #diff_process
                #persist_op

                unsafe { self.0.data.with_mut_ref(link, |archived| {
                    #(#row_updates)*
                }).map_err(WorkTableError::PagesError)? };

                lock.unlock();
                self.0.lock_map.remove_with_lock_check(&pk).await;

                #persist_call

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
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());
        let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();

        let size_check = if let Some(f) = unsized_fields {
            let fields_check: Vec<_> = f
                .iter()
                .map(|f| {
                    let fn_ident = Ident::new(format!("get_{f}_size").as_str(), Span::call_site());
                    quote! {
                        need_to_reinsert |= archived_row.#fn_ident() > self.#fn_ident(link)?;
                    }
                })
                .collect();
            let row_updates = idents
                .iter()
                .map(|i| {
                    quote! {
                        row_new.#i = row.#i.clone();
                    }
                })
                .collect::<Vec<_>>();
            let full_row_lock = self.gen_full_lock_for_update();

            quote! {
                let mut need_to_reinsert = false;
                #(#fields_check)*
                if need_to_reinsert {
                    let op_lock = locks.remove(&pk).expect("should not be deleted as links are unique");
                    op_lock.unlock();
                    let lock = {
                        #full_row_lock
                    };
                    let row_old = self.select(pk.clone()).expect("should not be deleted by other thread");
                    let mut row_new = row_old.clone();
                    #(#row_updates)*
                    self.reinsert(row_old, row_new)?;

                    lock.unlock();  // Releases locks
                    self.0.lock_map.remove_with_lock_check(&pk).await; // Removes locks
                } else {
                    pk_to_unlock.insert(pk.clone(), locks.remove(&pk).expect("should not be deleted as links are unique"));
                }
            }
        } else {
            quote! {}
        };
        let diff_process = self.gen_process_diffs_on_index(idents, idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let by = if is_float(by_ident.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                let links: Vec<_> = self.0.indexes.#index.get(#by).map(|(_, l)| *l).collect();

                let mut locks = std::collections::HashMap::new();
                for link in links.iter() {
                    let pk = self.0.data.select(*link)?.get_primary_key().clone();
                    let op_lock = {
                        #custom_lock
                    };
                    locks.insert(pk, op_lock);
                }

                let links: Vec<_> = self.0.indexes.#index.get(#by).map(|(_, l)| *l).collect();
                let mut pk_to_unlock: std::collections::HashMap<_, std::sync::Arc<Lock>> = std::collections::HashMap::new();
                let op_id = OperationId::Multi(uuid::Uuid::now_v7());
                for link in links.into_iter() {
                    let pk = self.0.data.select(link)?.get_primary_key().clone();
                    let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                        .map_err(|_| WorkTableError::SerializeError)?;

                    let mut archived_row = unsafe {
                        rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                            .unseal_unchecked()
                    };

                    #size_check
                    #diff_process
                    #persist_op

                    unsafe {
                        self.0.data.with_mut_ref(link, |archived| {
                            #(#row_updates)*
                        }).map_err(WorkTableError::PagesError)?;
                    }

                    #persist_call
                }
                for (pk, lock) in pk_to_unlock {
                    lock.unlock();
                    self.0.lock_map.remove_with_lock_check(&pk).await;
                }
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
        idx_idents: Option<&Vec<Ident>>,
        unsized_fields: Option<Vec<&Ident>>,
    ) -> TokenStream {
        let method_ident = Ident::new(
            format!("update_{snake_case_name}").as_str(),
            Span::mixed_site(),
        );

        let query_ident = Ident::new(format!("{name}Query").as_str(), Span::mixed_site());
        let by_ident = Ident::new(format!("{name}By").as_str(), Span::mixed_site());
        let lock_ident = WorktableNameGenerator::get_update_query_lock_ident(&snake_case_name);

        let row_updates = idents
            .iter()
            .map(|i| {
                quote! {
                    std::mem::swap(&mut archived.inner.#i, &mut archived_row.#i);
                }
            })
            .collect::<Vec<_>>();
        let size_check = self.gen_size_check(unsized_fields, idents);
        let diff_process = self.gen_process_diffs_on_index(idents, idx_idents);
        let persist_call = self.gen_persist_call();
        let persist_op = self.gen_persist_op();
        let by = if is_float(by_ident.to_string().as_str()) {
            quote! {
                &OrderedFloat(by)
            }
        } else {
            quote! {
                &by
            }
        };
        let custom_lock = self.gen_custom_lock_for_update(lock_ident);

        quote! {
            pub async fn #method_ident(&self, row: #query_ident, by: #by_ident) -> core::result::Result<(), WorkTableError> {
                 let mut bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)
                    .map_err(|_| WorkTableError::SerializeError)?;

                let mut archived_row = unsafe {
                    rkyv::access_unchecked_mut::<<#query_ident as rkyv::Archive>::Archived>(&mut bytes[..])
                        .unseal_unchecked()
                };

                let link = self.0.indexes.#index
                    .get(#by)
                    .map(|kv| kv.get().value)
                    .ok_or(WorkTableError::NotFound)?;
                let pk = self.0.data.select(link)?.get_primary_key().clone();

                let lock = {
                    #custom_lock
                };

                let link = self.0.indexes.#index
                    .get(#by)
                    .map(|kv| kv.get().value)
                    .ok_or(WorkTableError::NotFound)?;

                let op_id = OperationId::Single(uuid::Uuid::now_v7());
                #size_check
                #diff_process
                #persist_op

                unsafe {
                    self.0.data.with_mut_ref(link, |archived| {
                        #(#row_updates)*
                    }).map_err(WorkTableError::PagesError)?;
                }

                lock.unlock();
                self.0.lock_map.remove_with_lock_check(&pk).await;

                #persist_call

                core::result::Result::Ok(())
            }
        }
    }
}
