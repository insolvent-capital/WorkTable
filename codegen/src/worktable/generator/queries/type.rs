use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let rows: Vec<_> = self
            .columns
            .indexes
            .iter()
            .map(|(_i, idx)| self.columns.columns_map.get(&idx.field))
            .into_iter()
            .map(|t| {
                let type_upper = Ident::new(
                    &t.expect("REASON").to_string().to_uppercase(),
                    Span::mixed_site(),
                );

                Ok::<_, syn::Error>(quote! {

                    #type_upper(#t),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let type_ident = Ident::new(format!("AvailableTypes").as_str(), Span::mixed_site());
        let diff_ident = Ident::new(format!("Difference").as_str(), Span::mixed_site());

        let defs = Ok::<_, syn::Error>(quote! {
            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
            #[repr(C)]
            pub enum #type_ident {
                #(#rows)*
            }

            #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
            #[repr(C)]
            pub struct #diff_ident {
                old_value: #type_ident,
                new_value: #type_ident,
            }
        });

        let t = defs?;
        println!("{}", t.to_string());
        Ok(t)
    }

    pub fn gen_result_types_def(&mut self) -> syn::Result<TokenStream> {
        if let Some(queries) = &self.queries {
            let query_defs = queries
                .updates
                .keys()
                .map(|v| {
                    let ident = Ident::new(format!("{}Query", v).as_str(), Span::mixed_site());
                    let rows = queries
                        .updates
                        .get(v)
                        .expect("exists")
                        .columns
                        .iter()
                        .map(|i| {
                            let type_ = self
                                .columns
                                .columns_map
                                .get(i)
                                .ok_or(syn::Error::new(i.span(), "Unexpected column name"))?;

                            Ok::<_, syn::Error>(quote! {
                                pub #i: #type_,
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok::<_, syn::Error>(quote! {

                        #[derive(rkyv::Archive, Debug, rkyv::Deserialize, Clone, rkyv::Serialize)]
                        #[repr(C)]
                        pub struct #ident {
                            #(#rows)*
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let by_defs = queries
                .updates
                .values()
                .map(|op| {
                    let ident = Ident::new(format!("{}By", &op.name).as_str(), Span::mixed_site());
                    let field_type = self
                        .columns
                        .columns_map
                        .get(&op.by)
                        .ok_or(syn::Error::new(op.by.span(), "Unexpected column name"))?;

                    Ok::<_, syn::Error>(quote! {
                        pub type #ident = #field_type;
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(quote! {
                #(#query_defs)*
                #(#by_defs)*
            })
        } else {
            Ok(quote! {})
        }
    }
}
