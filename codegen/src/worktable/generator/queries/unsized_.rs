use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_unsized_impls(&self) -> TokenStream {
        if self.columns.is_sized {
            quote! {}
        } else {
            let unsized_field_len_fns = self.gen_get_unsized_field_len_wt_fn();
            let unsized_field_len_query_fns = self.gen_get_unsized_field_len_query_fn();
            quote! {
                #unsized_field_len_fns
                #unsized_field_len_query_fns
            }
        }
    }

    fn gen_get_unsized_field_len_wt_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let table_ident = name_generator.get_work_table_ident();

        let unsized_fields: Vec<_> = self
            .columns
            .columns_map
            .iter()
            .filter_map(|(k, v)| {
                if v.to_string() == "String" {
                    Some(k)
                } else {
                    None
                }
            })
            .map(|f| {
                let fn_ident = Ident::new(format!("get_{f}_size").as_str(), Span::call_site());
                quote! {
                    fn #fn_ident(&self, link: Link) -> core::result::Result<usize, WorkTableError> {
                        self.0.data
                            .with_ref(link, |row_ref| row_ref.inner.#f.len())
                            .map_err(WorkTableError::PagesError)
                    }
                }
            })
            .collect();

        quote! {
            impl #table_ident {
                #(#unsized_fields)*
            }
        }
    }

    fn gen_get_unsized_field_len_query_fn(&self) -> TokenStream {
        if let Some(q) = &self.queries {
            let query_impls: Vec<_> = q
                .updates
                .iter()
                .filter(|(_, op)| {
                    op.columns
                        .iter()
                        .any(|c| self.columns.columns_map.get(c).unwrap().to_string() == "String")
                })
                .map(|(i, op)| {
                    let archived_ident =
                        Ident::new(format!("Archived{i}Query").as_str(), Span::call_site());
                    let unsized_fields: Vec<_> = op
                        .columns
                        .iter()
                        .filter(|c| {
                            self.columns.columns_map.get(c).unwrap().to_string() == "String"
                        })
                        .map(|c| {
                            let fn_ident =
                                Ident::new(format!("get_{c}_size").as_str(), Span::call_site());
                            quote! {
                                pub fn #fn_ident(&self) -> usize {
                                    self.#c.as_str().to_string().aligned_size()
                                }
                            }
                        })
                        .collect();

                    quote! {
                        impl #archived_ident {
                            #(#unsized_fields)*
                        }
                    }
                })
                .collect();

            quote! {
                #(#query_impls)*
            }
        } else {
            quote! {}
        }
    }
}
