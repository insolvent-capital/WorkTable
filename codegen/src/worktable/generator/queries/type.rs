use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_result_types_def(&mut self) -> syn::Result<TokenStream> {
        if let Some(queries) = &self.queries {
            let defs = queries.updates.keys().map(|v| {
                let ident = Ident::new(format!("{}Query", v).as_str(), Span::mixed_site());
                let rows = queries.updates.get(v).expect("exists").columns.iter().map(|i| {
                    let type_ = self.columns.columns_map.get(i).ok_or(syn::Error::new(
                        i.span(),
                        "Unexpected column name",
                    ))?;

                    Ok::<_, syn::Error>(quote! {
                        pub #i: #type_,
                    })
                }).collect::<Result<Vec<_>, _>>()?;

                Ok::<_, syn::Error>(quote! {
                    #[derive(Debug, Clone)]
                    pub struct #ident {
                        #(#rows)*
                    }
                })
            })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(quote! {
                #(#defs)*
            })
        } else {
            Ok(quote! {})
        }
    }
}