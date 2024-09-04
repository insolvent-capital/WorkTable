use proc_macro2::TokenStream;
use quote::quote;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_query_update_impl(&mut self) -> syn::Result<TokenStream> {
        if let Some(q) = &self.queries {
            todo!()
        } else {
            Ok(quote! {})
        }
    }
}