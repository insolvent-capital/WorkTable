use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
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
        let upsert_fn = self.gen_table_upsert_fn();
        let get_next_fn = self.gen_table_get_next_fn();
        let iter_with_fn = self.gen_table_iter_with_fn();
        let iter_with_async_fn = self.gen_table_iter_with_async_fn();

        quote! {
            impl #ident {
                #new_fn
                #name_fn
                #select_fn
                #insert_fn
                #upsert_fn
                #get_next_fn
                #iter_with_fn
                #iter_with_async_fn
            }
        }
    }

    fn gen_table_new_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_name = name_generator.get_work_table_literal_name();

        if self.is_persist {
            quote! {
                pub fn new(manager:  std::sync::Arc<DatabaseManager>) -> Self {
                    let mut inner = WorkTable::default();
                    inner.table_name = #table_name;
                    Self(inner, manager)
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
            pub fn select(&self, pk: #primary_key_type) -> Option<#row_type> {
                self.0.select(pk)
            }
        }
    }

    fn gen_table_insert_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();

        quote! {
            pub fn insert(&self, row: #row_type) -> core::result::Result<#primary_key_type, WorkTableError> {
                self.0.insert(row)
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
}
