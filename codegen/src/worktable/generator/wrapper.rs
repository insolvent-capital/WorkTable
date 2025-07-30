use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub fn gen_wrapper_def(&self) -> TokenStream {
        let type_ = self.gen_wrapper_type();
        let impl_ = self.gen_wrapper_impl();
        let storable_impl = self.get_wrapper_storable_impl();
        let ghost_wrapper_impl = self.get_wrapper_ghost_impl();

        quote! {
            #type_
            #impl_
            #storable_impl
            #ghost_wrapper_impl
        }
    }

    fn gen_wrapper_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_row_type_ident();
        let wrapper_ident = name_generator.get_wrapper_type_ident();

        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, rkyv::Serialize)]
            #[repr(C)]
            pub struct #wrapper_ident {
                inner: #row_ident,
                is_ghosted: bool,
                is_deleted: bool,
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

                fn is_ghosted(&self) -> bool {
                    self.is_ghosted
                }

                fn from_inner(inner: #row_ident) -> Self {
                    Self {
                        inner,
                        is_ghosted: true,
                        is_deleted: false,
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

    fn get_wrapper_ghost_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_ident = name_generator.get_archived_wrapper_type_ident();

        quote! {
            impl GhostWrapper for #row_ident {
                fn unghost(&mut self) {
                    self.is_ghosted = false;
                }
            }
        }
    }
}
