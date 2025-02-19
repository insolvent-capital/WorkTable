use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_wrapper_def(&self) -> TokenStream {
        let type_ = self.gen_wrapper_type();
        let impl_ = self.gen_wrapper_impl();
        let storable_impl = self.get_wrapper_storable_impl();

        println!("!TYPE {}", type_);

        quote! {
            #type_
            #impl_
            #storable_impl
        }
    }

    fn gen_wrapper_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let wrapper_ident = name_generator.get_wrapper_type_ident();
        let lock_ident = name_generator.get_lock_type_ident();

        let row_locks = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #name: Option<std::sync::Arc<Lock>>,
                }
            })
            .collect::<Vec<_>>();

        let row_new = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let col = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #col: None
                }
            })
            .collect::<Vec<_>>();

        let lock_await = self
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
            .collect::<Vec<_>>();

        let row_unlock = self
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
            .collect::<Vec<_>>();

        let row_lock = self
            .columns
            .columns_map
            .keys()
            .map(|col| {
                let col = Ident::new(format!("{}_lock", col).as_str(), Span::mixed_site());
                quote! {
                     if let Some(#col) = &self.#col {
                        #col.lock();
                     }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
            #[repr(C)]
            pub struct #wrapper_ident {
                inner: #row_ident,
                is_deleted: bool,
            }

            #[derive(Debug)]
             pub struct #lock_ident {
                lock: Option<std::sync::Arc<Lock>>,
                #(#row_locks)*
            }

            impl #lock_ident {
                pub fn new() -> Self {
                    Self {
                        lock: None,
                        #(#row_new),*
                    }
                }

                pub fn lock(&self) {
                    if let Some(lock) = &self.lock {
                        lock.lock();
                    }
                    #(#row_lock)*
                }


                pub fn unlock(&self) {
                    if let Some(lock) = &self.lock {
                        lock.unlock();
                    }
                    #(#row_unlock)*
                }

                pub async fn lock_await(&self) {
                    let mut futures = Vec::new();

                    if let Some(lock) = &self.lock {
                        futures.push(lock.as_ref());
                    }
                    #(#lock_await)*
                    futures::future::join_all(futures).await;
                }
            }
        }
    }

    pub fn gen_wrapper_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let wrapper_ident = name_generator.get_wrapper_type_ident();
        let row_ident = name_generator.get_row_type_ident();

        quote! {

            impl RowWrapper<#row_ident> for #wrapper_ident {
                fn get_inner(self) -> #row_ident {
                    self.inner
                }

                fn from_inner(inner: #row_ident) -> Self {
                    Self {
                        inner,
                        is_deleted: Default::default(),
                    }
                }
            }
        }
    }

    fn get_wrapper_storable_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let wrapper_ident = name_generator.get_wrapper_type_ident();

        quote! {
            impl StorableRow for #row_ident {
                type WrappedRow = #wrapper_ident;
            }
        }
    }
}
