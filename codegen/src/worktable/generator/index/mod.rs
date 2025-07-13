mod cdc;
mod info;
mod usual;

use crate::name_generator::{is_float, is_unsized, WorktableNameGenerator};
use crate::worktable::generator::Generator;
use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    /// Generates index type and it's impls.
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_secondary_index_impl_def();
        let info_def = self.gen_secondary_index_info_impl_def();
        let cdc_impl_def = if self.is_persist {
            self.gen_secondary_index_cdc_impl_def()
        } else {
            quote! {}
        };
        let default_impl = self.gen_index_default_impl();
        let available_indexes = self.gen_available_indexes();

        quote! {
            #type_def
            #impl_def
            #info_def
            #cdc_impl_def
            #default_impl
            #available_indexes
        }
    }

    /// Generates table's secondary index struct definition. It has fields with index names and types varying on index
    /// uniqueness. For unique index it's `TreeIndex<T, Link`, for non-unique `TreeIndex<T, Arc<LockFreeSet<Link>>>`.
    /// Index also derives `PersistIndex` and `MemStat` macro.
    fn gen_type_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_index_type_ident();
        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let t = self.columns.columns_map.get(i).unwrap();
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                if idx.is_unique {
                    if is_unsized(&t.to_string()) {
                        quote! {
                            #i: IndexMap<#t, Link, UnsizedNode<IndexPair<#t, Link>>>
                        }
                    } else {
                        quote! {#i: IndexMap<#t, Link>}
                    }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap<#t, Link, UnsizedNode<IndexMultiPair<#t, Link>>>}
                    } else {
                        quote! {#i: IndexMultiMap<#t, Link>}
                    }
                }
            })
            .collect::<Vec<_>>();

        let derive = if self.is_persist {
            quote! {
                #[derive(Debug, MemStat, PersistIndex)]
            }
        } else {
            quote! {
                #[derive(Debug, MemStat)]
            }
        };

        quote! {
            #derive
            pub struct #ident {
                #(#index_rows),*
            }
        }
    }

    fn gen_index_default_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();

        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let t = self.columns.columns_map.get(i).unwrap();
                let t = if is_float(t.to_string().as_str()) {
                    quote! { OrderedFloat<#t> }
                } else {
                    quote! { #t }
                };
                let i = &idx.name;

                #[allow(clippy::collapsible_else_if)]
                if idx.is_unique {
                    if is_unsized(&t.to_string()) {
                        quote! {
                            #i: IndexMap::with_maximum_node_size(#const_name),
                        }
                    } else {
                        quote! {#i: IndexMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name)),}
                    }
                } else {
                    if is_unsized(&t.to_string()) {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(#const_name), }
                    } else {
                        quote! {#i: IndexMultiMap::with_maximum_node_size(get_index_page_size_from_data_length::<#t>(#const_name)),}
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            impl Default for #index_type_ident {
                fn default() -> Self {
                    Self {
                        #(#index_rows)*
                    }
                }
            }
        }
    }

    fn gen_available_indexes(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_indexes_ident();

        let indexes = self.columns.indexes.values().map(|i| {
            let camel_case_name = i
                .name
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let i: TokenStream = camel_case_name.parse().unwrap();
            quote! {
                #i,
            }
        });

        if self.columns.indexes.is_empty() {
            quote! {
                pub type #avt_type_ident = ();
            }
        } else {
            quote! {
                #[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Hash, Eq)]
                pub enum #avt_type_ident {
                    #(#indexes)*
                }
            }
        }
    }
}
