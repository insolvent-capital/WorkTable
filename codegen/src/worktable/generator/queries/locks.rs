use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_query_locks_impl(&mut self) -> syn::Result<TokenStream> {
        if let Some(q) = &self.queries {
            let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
            let lock_type_ident = name_generator.get_lock_type_ident();

            let fns = q
                .updates
                .keys()
                .map(|name| {
                    let snake_case_name = name
                        .to_string()
                        .from_case(Case::Pascal)
                        .to_case(Case::Snake);

                    let lock_await_ident = Ident::new(
                        format!("lock_await_{snake_case_name}").as_str(),
                        Span::mixed_site(),
                    );

                    let locks_await = q
                        .updates
                        .get(name)
                        .expect("exists")
                        .columns
                        .iter()
                        .map(|col| {
                            let col =
                                Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                            quote! {
                               if let Some(lock) = &self.#col {
                                    futures.push(lock.as_ref());
                               }
                            }
                        })
                        .collect::<Vec<_>>();

                    quote! {

                        pub async fn #lock_await_ident(&self) {
                            let mut futures = Vec::new();

                            if let Some(lock) = &self.lock {
                                futures.push(lock.as_ref());
                            }


                            #(#locks_await)*
                            futures::future::join_all(futures).await;
                        }
                    }
                })
                .collect::<Vec<_>>();

            Ok(quote! {
                impl #lock_type_ident {
                    #(#fns)*
                }
            })
        } else {
            Ok(quote! {})
        }
    }
}
