use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let rows: Vec<_> = self
            .columns
            .indexes
            .iter()
            .filter_map(|(_, idx)| self.columns.columns_map.get(&idx.field))
            .map(|s| {
                let type_ident = Ident::new(s.to_string().as_str(), Span::mixed_site());
                let type_upper =
                    Ident::new(s.to_string().to_uppercase().as_str(), Span::mixed_site());
                Some(quote! {
                    #[from]
                    #type_upper(#type_ident),
                })
            })
            .collect();

        if !rows.is_empty() {
            Ok(quote! {
                #[derive(rkyv::Archive, Debug, derive_more::Display, rkyv::Deserialize, Clone, rkyv::Serialize)]
                #[derive(From, PartialEq)]
                #[non_exhaustive]
                pub enum #avt_type_ident {
                    #(#rows)*
                }
            })
        } else {
            Ok(quote! {
                type #avt_type_ident = ();
            })
        }
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
