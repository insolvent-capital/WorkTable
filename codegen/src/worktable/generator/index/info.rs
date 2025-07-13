use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_secondary_index_info_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();

        let info_fn = self.gen_index_info_fn();
        let is_empty_fn = self.gen_index_is_empty_fn();

        quote! {
            impl TableSecondaryIndexInfo for #index_type_ident {
                #info_fn
                #is_empty_fn
            }
        }
    }

    fn gen_index_info_fn(&self) -> TokenStream {
        let rows = self.columns.indexes.values().map(|idx| {
            let index_field_name = &idx.name;
            let index_name_str = index_field_name.to_string();

            if idx.is_unique {
                quote! {
                    info.push(IndexInfo {
                        name: #index_name_str.to_string(),
                        index_type: IndexKind::Unique,
                        key_count: self.#index_field_name.len(),
                        capacity: self.#index_field_name.capacity(),
                        heap_size: self.#index_field_name.heap_size(),
                        used_size: self.#index_field_name.used_size(),
                        node_count: self.#index_field_name.node_count(),
                    });
                }
            } else {
                quote! {
                    info.push(IndexInfo {
                        name: #index_name_str.to_string(),
                        index_type: IndexKind::NonUnique,
                        key_count: self.#index_field_name.len(),
                        capacity: self.#index_field_name.capacity(),
                        heap_size: self.#index_field_name.heap_size(),
                        used_size: self.#index_field_name.used_size(),
                        node_count: self.#index_field_name.node_count(),
                    });
                }
            }
        });

        quote! {
            fn index_info(&self) -> Vec<IndexInfo> {
                let mut info = Vec::new();
                #(#rows)*
                info
            }
        }
    }

    fn gen_index_is_empty_fn(&self) -> TokenStream {
        let is_empty = self
            .columns
            .indexes
            .values()
            .map(|idx| {
                let index_field_name = &idx.name;
                quote! {
                    self.#index_field_name.len() == 0
                }
            })
            .collect::<Vec<_>>();

        if is_empty.is_empty() {
            quote! {
                fn is_empty(&self) -> bool {
                    true
                }
            }
        } else {
            quote! {
                fn is_empty(&self) -> bool {
                    #(#is_empty) &&*
                }
            }
        }
    }
}
