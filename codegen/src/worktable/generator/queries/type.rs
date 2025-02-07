use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use std::collections::HashSet;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let types: Vec<_> = self
            .columns
            .indexes
            .iter()
            .map(|(_i, idx)| self.columns.columns_map.get(&idx.field))
            .into_iter()
            .filter_map(|t| t)
            .map(|s| s.to_string())
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|t| {
                let type_ = Ident::new(&t.to_string(), Span::mixed_site());
                Ok::<_, syn::Error>(quote! {
                    #type_
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let types = if types.is_empty() {
            vec![quote! {String}]
        } else {
            types
        };

        let rows = types
            .iter()
            .map(|t| {
                let type_upper = Ident::new(&t.to_string().to_uppercase(), Span::mixed_site());
                Ok::<_, syn::Error>(quote! {
                    #[from]
                    #type_upper(#t)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let default_variant = if types.iter().any(|r| r.to_string().contains("String")) {
            quote! { #avt_type_ident::STRING(String::default()) }
        } else if let Some(first_variant) = types.first() {
            let type_upper = Ident::new(
                &first_variant.to_string().to_uppercase(),
                Span::mixed_site(),
            );
            quote! { #avt_type_ident::#type_upper(Default::default()) }
        } else {
            quote! {}
        };

        Ok::<_, syn::Error>(quote! {
            #[derive(rkyv::Archive, Debug, derive_more::Display, rkyv::Deserialize, Clone, rkyv::Serialize)]
            #[derive(From, PartialEq)]
            pub enum #avt_type_ident {
                #(#rows),*
            }

            impl Default for #avt_type_ident {
                 fn default() -> Self {

                  #default_variant

                 }
            }
        })
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
