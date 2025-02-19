use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_wrapper_def(&self) -> TokenStream {
        let type_ = self.gen_wrapper_type();
        let impl_ = self.gen_wrapper_impl();
        let archived_impl = self.get_wrapper_archived_impl();
        let storable_impl = self.get_wrapper_storable_impl();

        println!("!TYPE {}", type_);
        println!("!Archived {}", archived_impl);

        quote! {
            #type_
            #impl_
            #archived_impl
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
                    #name: u16,
                }
            })
            .collect::<Vec<_>>();

        let row_locks2 = self
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
        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
            #[repr(C)]
            pub struct #wrapper_ident {
                inner: #row_ident,
                is_deleted: bool,
                lock: u16,
                #(#row_locks)*
            }
            #[derive(Debug)]
             pub struct #lock_ident {
                lock: Option<std::sync::Arc<Lock>>,
                #(#row_locks2)*
            }
        }
    }

    pub fn gen_wrapper_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let wrapper_ident = name_generator.get_wrapper_type_ident();
        let row_ident = name_generator.get_row_type_ident();

        let row_defaults = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    #name: Default::default(),
                }
            })
            .collect::<Vec<_>>();

        quote! {

            impl RowWrapper<#row_ident> for #wrapper_ident {
                fn get_inner(self) -> #row_ident {
                    self.inner
                }

                fn from_inner(inner: #row_ident) -> Self {
                    Self {
                        inner,
                        is_deleted: Default::default(),
                        lock: Default::default(),
                        #(#row_defaults)*
                    }
                }
            }
        }
    }

    fn get_wrapper_archived_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let wrapper_ident = name_generator.get_wrapper_type_ident();
        let lock_ident = name_generator.get_lock_type_ident();

        let archived_wrapper_ident = Ident::new(
            format!("Archived{}", &wrapper_ident).as_str(),
            Span::mixed_site(),
        );
        let checks = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    if self.#name != 0 {
                        return Some(self.#name.into());
                    }
                }
            })
            .collect::<Vec<_>>();

        let checks2 = self
            .columns
            .columns_map
            .keys()
            .map(|i| {
                let name = Ident::new(format!("{i}_lock").as_str(), Span::mixed_site());
                quote! {
                    if let Some(#name) = &self.#name {
                        if #name.locked.load(std::sync::atomic::Ordering::Acquire) {
                            return true;
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {

            impl Lockable for #lock_ident {
                fn is_locked(&self) -> bool {
                    if let Some(lock) = &self.lock {
                        if lock.locked.load(std::sync::atomic::Ordering::Acquire) {
                            return true;
                        }
                    }

                    #(#checks2)*

                    false
                }
            }

            impl ArchivedRow for #archived_wrapper_ident {
                fn is_locked(&self) -> Option<u16> {
                    if self.lock != 0 {
                        return Some(self.lock.into());
                    }
                    #(#checks)*
                    None
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
