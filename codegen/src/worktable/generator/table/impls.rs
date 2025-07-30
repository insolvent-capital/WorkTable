use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::{is_unsized_vec, WorktableNameGenerator};
use crate::worktable::generator::Generator;
use crate::worktable::model::GeneratorType;

impl Generator {
    pub fn gen_table_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();

        let new_fn = self.gen_table_new_fn();
        let name_fn = self.gen_table_name_fn();
        let select_fn = self.gen_table_select_fn();
        let insert_fn = self.gen_table_insert_fn();
        let reinsert_fn = self.gen_table_reinsert_fn();
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
        let iter_with_fn = self.gen_table_iter_with_fn();
        let iter_with_async_fn = self.gen_table_iter_with_async_fn();
        let count_fn = self.gen_table_count_fn();
        let system_info_fn = self.gen_system_info_fn();

        quote! {
            impl #ident {
                #new_fn
                #name_fn
                #select_fn
                #insert_fn
                #reinsert_fn
                #upsert_fn
                #count_fn
                #get_next_fn
                #iter_with_fn
                #iter_with_async_fn
                #system_info_fn
            }
        }
    }

    fn gen_table_new_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_name = name_generator.get_work_table_literal_name();
        let engine = name_generator.get_persistence_engine_ident();
        let task = name_generator.get_persistence_task_ident();
        let dir_name = name_generator.get_dir_name();
        let pk_type = name_generator.get_primary_key_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();

        if self.is_persist {
            let pk_types = &self
                .columns
                .primary_keys
                .iter()
                .map(|i| {
                    self.columns
                        .columns_map
                        .get(i)
                        .expect("should exist as got from definition")
                        .to_string()
                })
                .collect::<Vec<_>>();
            let pk_types_unsized = is_unsized_vec(pk_types);
            let index_size = if pk_types_unsized {
                quote! {
                    let size = #const_name;
                }
            } else {
                quote! {
                    let size = get_index_page_size_from_data_length::<#pk_type>(#const_name);
                }
            };
            quote! {
                pub async fn new(config: PersistenceConfig) -> eyre::Result<Self> {
                    let mut inner = WorkTable::default();
                    inner.table_name = #table_name;
                    #index_size
                    inner.pk_map = IndexMap::with_maximum_node_size(size);
                    let table_files_path = format!("{}/{}", config.tables_path, #dir_name);
                    let engine: #engine = PersistenceEngine::from_table_files_path(table_files_path).await?;
                    core::result::Result::Ok(Self(
                        inner,
                        config,
                        #task::run_engine(engine)
                    ))
                }
            }
        } else {
            quote! {}
        }
    }

    fn gen_table_name_fn(&self) -> TokenStream {
        quote! {
            pub fn name(&self) -> &'static str {
                &self.0.table_name
            }
        }
    }

    fn gen_table_select_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn select<Pk>(&self, pk: Pk) -> Option<#row_type>
            where #primary_key_type: From<Pk> {
                self.0.select(pk.into())
            }
        }
    }

    fn gen_table_insert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        let insert = if self.is_persist {
            quote! {
                let (pk, op) = self.0.insert_cdc(row)?;
                self.2.apply_operation(op);
                core::result::Result::Ok(pk)
            }
        } else {
            quote! {
                self.0.insert(row)
            }
        };

        quote! {
            pub fn insert(&self, row: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                #insert
            }
        }
    }

    fn gen_table_reinsert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        let reinsert = if self.is_persist {
            quote! {
                let (pk, op) = self.0.reinsert_cdc(row_old, row_new)?;
                self.2.apply_operation(op);
                core::result::Result::Ok(pk)
            }
        } else {
            quote! {
                self.0.reinsert(row_old, row_new)
            }
        };

        quote! {
            pub fn reinsert(&self, row_old: #row_type, row_new: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                #reinsert
            }
        }
    }

    fn gen_table_upsert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();

        quote! {
            pub async fn upsert(&self, row: #row_type) -> core::result::Result<(), WorkTableError> {
                let pk = row.get_primary_key();
                let need_to_update = {
                    if let Some(_) = self.0.pk_map.get(&pk)
                    {
                        true
                    } else {
                        false
                    }
                };
                if need_to_update {
                    self.update(row).await?;
                } else {
                    self.insert(row)?;
                }
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_table_get_next_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let primary_key_type = name_generator.get_primary_key_type_ident();

        match self.columns.generator_type {
            GeneratorType::Custom | GeneratorType::Autoincrement => {
                quote! {
                    pub fn get_next_pk(&self) -> #primary_key_type {
                        self.0.get_next_pk()
                    }
                }
            }
            GeneratorType::None => {
                quote! {}
            }
        }
    }

    fn gen_table_iter_with_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
            f(data)?;
        });

        quote! {
            pub fn iter_with<
                F: Fn(#row_type) -> core::result::Result<(), WorkTableError>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_with_async_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let inner = self.gen_table_iter_inner(quote! {
             f(data).await?;
        });

        quote! {
            pub async fn iter_with_async<
                F: Fn(#row_type) -> Fut,
                Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>
            >(&self, f: F) -> core::result::Result<(), WorkTableError> {
                #inner
            }
        }
    }

    fn gen_table_iter_inner(&self, func: TokenStream) -> TokenStream {
        quote! {
            let first = self.0.pk_map.iter().next().map(|(k, v)| (k.clone(), *v));
            let Some((mut k, link)) = first else {
                return Ok(())
            };

            let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
            #func

            let mut ind = false;
            while !ind {
                let next = {
                    let mut iter = self.0.pk_map.range(k.clone()..);
                    let next = iter.next().map(|(k, v)| (k.clone(), *v)).filter(|(key, _)| key != &k);
                    if next.is_some() {
                        next
                    } else {
                        iter.next().map(|(k, v)| (k.clone(), *v))
                    }
                };
                if let Some((key, link)) = next {
                    let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                   #func
                    k = key
                } else {
                    ind = true;
                };
            }

            core::result::Result::Ok(())
        }
    }

    fn gen_table_count_fn(&self) -> TokenStream {
        quote! {
            pub fn count(&self) -> usize {
                let count = self.0.pk_map.len();
                count
            }
        }
    }

    fn gen_system_info_fn(&self) -> TokenStream {
        quote! {
            pub fn system_info(&self) -> SystemInfo {
                self.0.system_info()
            }
        }
    }
}
