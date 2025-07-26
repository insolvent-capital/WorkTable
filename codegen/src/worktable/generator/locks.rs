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
                #(#rows)*
            }
        }
    }

    fn gen_locks_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let new_fn = self.gen_new_fn();
        let row_impl = self.gen_lock_row_impl();

        quote! {
            impl #lock_ident {
                #new_fn
            }

            #row_impl
        }
    }

    fn gen_lock_row_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let lock_ident = name_generator.get_lock_type_ident();

        let is_locked_fn = self.gen_is_locked_fn();
        let with_lock_fn = self.gen_with_lock_fn();
        let lock_fn = self.gen_lock_fn();
        let merge_fn = self.gen_merge_fn();

        quote! {
            impl RowLock for #lock_ident {
                #is_locked_fn
                #lock_fn
                #with_lock_fn
                #merge_fn
            }
        }
    }

    fn gen_is_locked_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! { self.#col.as_ref().map(|l| l.is_locked()).unwrap_or(false)  }
            })
            .collect();

        quote! {
            fn is_locked(&self) -> bool {
                #(#rows) ||*
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
            pub fn new() -> Self {
                Self {
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
                quote! { #col: Some(lock.clone()) }
            })
            .collect();

        quote! {
             fn with_lock(id: u16) -> (Self, std::sync::Arc<Lock>) {
                let lock = std::sync::Arc::new(Lock::new(id));
                (
                    Self {
                        #(#rows),*
                    },
                    lock
                )
            }
        }
    }

    fn gen_lock_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(lock) = &self.#col {
                        set.insert(lock.clone());
                    }
                    self.#col = Some(lock.clone());
                }
            })
            .collect();

        quote! {
            #[allow(clippy::mutable_key_type)]
             fn lock(&mut self, id: u16) -> (std::collections::HashSet<std::sync::Arc<Lock>>,  std::sync::Arc<Lock>) {
                let mut set = std::collections::HashSet::new();
                let lock = std::sync::Arc::new(Lock::new(id));
                #(#rows)*

                (set, lock)
            }
        }
    }

    fn gen_merge_fn(&self) -> TokenStream {
        let rows: Vec<_> = self
            .columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{col}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(#col) = &other.#col {
                        if self.#col.is_none() {
                            self.#col = Some(#col.clone());
                        } else {
                            set.insert(#col.clone());
                        }
                    }
                    other.#col = self.#col.clone();
                }
            })
            .collect();

        quote! {
            #[allow(clippy::mutable_key_type)]
            fn merge(&mut self, other: &mut Self) -> std::collections::HashSet<std::sync::Arc<Lock>> {
                let mut set = std::collections::HashSet::new();
                #(#rows)*
                set
            }
        }
    }
}
