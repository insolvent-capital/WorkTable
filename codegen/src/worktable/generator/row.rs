use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

impl Generator {
    /// Generates row type and it's impls.
    pub fn gen_row_def(&mut self) -> TokenStream {
        let def = self.gen_row_type();
        let table_row_impl = self.gen_row_table_row_impl();

        quote! {
            #def
            #table_row_impl
        }
    }

    /// Generates `TableRow` trait implementation for row.
    fn gen_row_table_row_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_row_type_ident();
        let primary_key_ident = name_generator.get_primary_key_type_ident();

        let primary_key = self
            .pk
            .clone()
            .expect("should be set in `Generator` at this point");
        let primary_key_columns_clone = if primary_key.values.len() == 1 {
            let pk_field = primary_key
                .values
                .keys()
                .next()
                .expect("should exist as length is checked");
            quote! {
                self.#pk_field.clone().into()
            }
        } else {
            let vals = primary_key
                .values
                .keys()
                .map(|i| {
                    quote! {
                        self.#i.clone()
                    }
                })
                .collect::<Vec<_>>();
            quote! {
                (#(#vals),*).into()
            }
        };

        quote! {
            impl TableRow<#primary_key_ident> for #ident {

                fn get_primary_key(&self) -> #primary_key_ident {
                    #primary_key_columns_clone
                }
            }
        }
    }

    /// Generates table's row struct definition. It has fields that were described in definition.
    fn gen_row_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_row_type_ident();

        let rows: Vec<_> = self
            .columns
            .columns_map
            .iter()
            .map(|(name, type_)| {
                quote! {pub #name: #type_,}
            })
            .collect();

        quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize, PartialEq)]
            #[rkyv(derive(Debug))]
            #[repr(C)]
            pub struct #ident {
                #(#rows)*
            }
        }
    }
}

// TODO: tests...
