use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_query_locks_impl(&mut self) -> syn::Result<TokenStream> {
        if let Some(q) = &self.queries {
            let wrapper_name = self.wrapper_name.as_ref().unwrap();
            let archived_wrapper = Ident::new(format!("Archived{}", &wrapper_name).as_str(), Span::mixed_site());

            let fns = q.updates.keys().map(|name| {
                let snake_case_name = name.to_string().from_case(Case::Pascal).to_case(Case::Snake);

                let check_ident = Ident::new(format!("check_{snake_case_name}_lock").as_str(), Span::mixed_site());
                let checks = q.updates.get(name).expect("exists").columns.iter().map(|col| {
                    let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                    quote! {
                        if self.#col != 0 {
                            return Some(self.#col);
                        }
                    }
                }).collect::<Vec<_>>();

                let lock_ident = Ident::new(format!("lock_{snake_case_name}").as_str(), Span::mixed_site());
                let locks = q.updates.get(name).expect("exists").columns.iter().map(|col| {
                    let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                    quote! {
                        self.#col = id;
                    }
                }).collect::<Vec<_>>();

                let unlock_ident = Ident::new(format!("unlock_{snake_case_name}").as_str(), Span::mixed_site());
                let unlocks = q.updates.get(name).expect("exists").columns.iter().map(|col| {
                    let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                    quote! {
                        self.#col = 0;
                    }
                }).collect::<Vec<_>>();

                let verify_ident = Ident::new(format!("verify_{snake_case_name}_lock").as_str(), Span::mixed_site());
                let verify = q.updates.get(name).expect("exists").columns.iter().map(|col| {
                    let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                    quote! {
                        if self.#col != id {
                            return false;
                        }
                    }
                }).collect::<Vec<_>>();

                quote! {
                    pub fn #check_ident(&self) -> Option<u16> {
                        #(#checks)*
                        None
                    }

                    pub unsafe fn #lock_ident(&mut self, id: u16) {
                        #(#locks)*
                    }

                    pub unsafe fn #unlock_ident(&mut self) {
                        #(#unlocks)*
                    }

                    pub fn #verify_ident(&self, id: u16) -> bool {
                        #(#verify)*
                        true
                    }
                }
            }).collect::<Vec<_>>();

            Ok(quote! {
                impl #archived_wrapper {
                    #(#fns)*
                }
            })
        } else {
            Ok(quote! {})
        }
    }
}