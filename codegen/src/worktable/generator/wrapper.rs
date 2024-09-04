
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_wrapper_def(&mut self) -> TokenStream {
        let name = &self.name;
        let row_name = self.row_name.as_ref().unwrap();
        let row_locks = self
            .columns
            .columns_map
            .iter()
            .map(|(i, _)| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #name: u16,
                }
            })
            .collect::<Vec<_>>();

        let wrapper_name = Ident::new(format!("{name}Wrapper").as_str(), Span::mixed_site());
        self.wrapper_name = Some(wrapper_name.clone());
        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
            #[repr(C)]
            pub struct #wrapper_name {
                inner: #row_name,

                is_deleted: bool,

                #(#row_locks)*
            }
        }
    }

    pub fn gen_wrapper_impl(&mut self) -> TokenStream {
        let row_name = self.row_name.as_ref().unwrap();
        let wrapper_name = self.wrapper_name.as_ref().unwrap();

        let storable_impl = quote! {
            impl StorableRow for #row_name {
                type WrappedRow = #wrapper_name;
            }
        };

        let row_sums = self
            .columns
            .columns_map
            .iter()
            .map(|(i, _)| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    self.#name
                }
            })
            .collect::<Vec<_>>();

        let archived_wrapper = Ident::new(format!("Archived{}", &wrapper_name).as_str(), Span::mixed_site());
        let archived_impl = quote! {
            impl ArchivedRow for #archived_wrapper {
                fn is_locked(&self) -> bool {
                    let sum =
                    #(#row_sums)+*;
                    sum == 0
                }
            }
        };

        let row_defaults = self
            .columns
            .columns_map
            .iter()
            .map(|(i, _)| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #name: Default::default(),
                }
            })
            .collect::<Vec<_>>();

        let wrapper_impl = quote! {
            impl RowWrapper<#row_name> for #wrapper_name {
                fn get_inner(self) -> #row_name {
                    self.inner
                }

                fn from_inner(inner: #row_name) -> Self {
                    Self {
                        inner,
                        is_deleted: Default::default(),
                        #(#row_defaults)*
                    }
                }
            }
        };

        quote! {
            #archived_impl
            #storable_impl
            #wrapper_impl
        }
    }
}
