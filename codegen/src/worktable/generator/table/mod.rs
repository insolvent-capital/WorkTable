use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::{WorktableNameGenerator, is_unsized_vec};
use crate::worktable::generator::Generator;

mod impls;
mod index_fns;
mod select_executor;

impl Generator {
    pub fn gen_table_def(&mut self) -> syn::Result<TokenStream> {
        let page_size_consts = self.gen_page_size_consts();
        let type_ = self.gen_table_type();
        let default = self.gen_table_default();
        let impl_ = self.gen_table_impl();
        let index_fns = self.gen_table_index_fns()?;
        let select_query_executor_impl = self.gen_table_select_query_executor_impl();
        let column_range_type = self.gen_table_column_range_type();

        Ok(quote! {
            #page_size_consts
            #type_
            #default
            #impl_
            #index_fns
            #select_query_executor_impl
            #column_range_type
        })
    }

    fn gen_table_default(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let table_name = name_generator.get_work_table_literal_name();

        if self.is_persist {
            quote! {}
        } else {
            quote! {
                 impl Default for #ident {
                    fn default() -> Self {
                        let mut inner = WorkTable::default();
                        inner.table_name = #table_name;
                        Self(inner)
                    }
                }
            }
        }
    }

    fn gen_table_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let index_type = name_generator.get_index_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();
        let persistence_task = name_generator.get_persistence_task_ident();
        let lock_ident = name_generator.get_lock_type_ident();

        let persist_type_part = if self.is_persist {
            quote! {
                , PersistenceConfig
                , #persistence_task
            }
        } else {
            quote! {}
        };

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
        let derive = if self.is_persist {
            if pk_types_unsized {
                quote! {
                    #[derive(Debug, PersistTable)]
                    #[table(pk_unsized)]
                }
            } else {
                quote! {
                    #[derive(Debug, PersistTable)]
                }
            }
        } else {
            quote! {
                 #[derive(Debug)]
            }
        };
        let node_type = if pk_types_unsized {
            quote! {
                UnsizedNode<IndexPair<#primary_key_type, Link>>
            }
        } else {
            quote! {
                Vec<IndexPair<#primary_key_type, Link>>
            }
        };

        if self.config.as_ref().and_then(|c| c.page_size).is_some() {
            quote! {
                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #avt_type_ident,
                        #avt_index_ident,
                        #index_type,
                        #lock_ident,
                        <#primary_key_type as TablePrimaryKey>::Generator,
                        #node_type,
                        #inner_const_name
                    >
                    #persist_type_part
                );
            }
        } else {
            quote! {
                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #primary_key_type,
                        #avt_type_ident,
                        #avt_index_ident,
                        #index_type,
                        #lock_ident,
                        <#primary_key_type as TablePrimaryKey>::Generator,
                        #node_type,
                    >
                    #persist_type_part
                );
            }
        }
    }

    fn gen_page_size_consts(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        if let Some(page_size) = &self.config.as_ref().and_then(|c| c.page_size) {
            let page_size = Literal::usize_unsuffixed(*page_size as usize);
            quote! {
                const #page_const_name: usize = #page_size;
                const #inner_const_name: usize = #page_size - GENERAL_HEADER_SIZE;
            }
        } else {
            quote! {
                const #page_const_name: usize = PAGE_SIZE;
                const #inner_const_name: usize = #page_const_name - GENERAL_HEADER_SIZE;
            }
        }
    }
}
