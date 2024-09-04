use proc_macro2::TokenStream;
use quote::quote;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_query_select_impl(&mut self) -> syn::Result<TokenStream> {
        let select_all = self.gen_select_all();

        let table_ident = self.table_name.as_ref().unwrap();
        Ok(quote! {
            impl #table_ident {
                #select_all
            }
        })
    }

    fn gen_select_all(&mut self) -> TokenStream {
        let row_ident = self.row_name.as_ref().unwrap();

        quote! {
            pub fn select_all(&self) -> Result<Vec<#row_ident>, WorkTableError> {
                let guard = Guard::new();
                self.0.pk_map.iter(&guard).map(|(_,l)| {
                    self.0.data.select(*l).map_err(WorkTableError::PagesError)
                }).collect::<Result<Vec<_>, _>>()
            }
        }
    }
}