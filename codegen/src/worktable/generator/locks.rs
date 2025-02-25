use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_locks_def(&self) -> TokenStream {
        let type_ = self.gen_locks_type();
        let impl_ = self.gen_locks_impl();

        quote! {
            #type_
            #impl_

        }
    }

    fn gen_locks_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #name: Option<std::sync::Arc<Lock>>, }
            })
            .collect();

        quote! {
             #[derive(Debug, Clone)]
             pub struct #lock_ident {
                id: u16,
                lock: Option<std::sync::Arc<Lock>>,
                #(#rows)*
            }
        }
    }

    fn gen_locks_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let new_fn = self.gen_new_fn();
        let with_lock_fn = self.gen_with_lock_fn();
        let lock_await_fn = self.gen_lock_await_fn();
        let unlock_fn = self.gen_unlock_fn();

        quote! {
            impl #lock_ident {
                #new_fn
                #with_lock_fn
                #lock_await_fn
                #unlock_fn
            }
        }
    }

    fn gen_new_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #col: None }
            })
            .collect();

        quote! {
            pub fn new(lock_id: u16) -> Self {
                Self {
                    id: lock_id,
                    lock: None,
                    #(#rows),*
                }
            }
        }
    }

    fn gen_with_lock_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { #col: Some(std::sync::Arc::new(Lock::new())) }
            })
            .collect();

        quote! {
             pub fn with_lock(lock_id: u16) -> Self {
                Self {
                    id: lock_id,
                    lock: Some(std::sync::Arc::new(Lock::new())),
                    #(#rows),*
                }
            }
        }
    }

    fn gen_lock_await_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                   if let Some(lock) = &self.#col {
                        futures.push(lock.as_ref());
                   }
                }
            })
            .collect();
        quote! {
             pub async fn lock_await(&self) {
                let mut futures = Vec::new();

                if let Some(lock) = &self.lock {
                    futures.push(lock.as_ref());
                }
                #(#rows)*
                futures::future::join_all(futures).await;
            }
        }
    }

    fn gen_unlock_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                     if let Some(#col) = &self.#col {
                        #col.unlock();
                     }
                }
            })
            .collect();

        quote! {
            pub fn unlock(&self) {
                if let Some(lock) = &self.lock {
                    lock.unlock();
                }
                #(#rows)*
            }

        }
    }
}
