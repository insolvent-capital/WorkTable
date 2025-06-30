use std::collections::HashSet;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

pub fn map_to_uppercase(str: &str) -> String {
    if str.contains("OrderedFloat") {
        let mut split = str.split("<");
        let _ = split.next();
        let inner_type = split
            .next()
            .expect("OrderedFloat def contains inner type")
            .replace(">", "");
        format!("Ordered{}", inner_type.to_uppercase().trim())
    } else {
        str.to_uppercase()
    }
}

impl Generator {
    pub fn gen_available_types_def(&mut self) -> syn::Result<TokenStream> {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let unique_types: HashSet<String> = self
            .columns
            .indexes
            .iter()
            .filter_map(|(_, idx)| self.columns.columns_map.get(&idx.field))
            .map(|ty| ty.to_string())
            .collect();

        let rows: Vec<_> = unique_types
            .iter()
            .map(|s| {
                let type_ident: TokenStream = s
                    .to_string()
                    .parse()
                    .expect("should be valid because parsed from declaration");
                let type_upper = map_to_uppercase(s);
                let type_upper = Ident::new(type_upper.as_str(), Span::mixed_site());
                Some(quote! {
                    #[from]
                    #type_upper(#type_ident),
                })
            })
            .collect();

        if !rows.is_empty() {
            Ok(quote! {
                #[derive(Clone, Debug, From,  PartialEq)]
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
                    let ident = Ident::new(format!("{v}Query").as_str(), Span::mixed_site());
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

                            let def = if type_.to_string().contains("OrderedFloat") {
                                let inner_type = type_.to_string();
                                let mut split = inner_type.split("<");
                                let _ = split.next();
                                let inner_type = split
                                    .next()
                                    .expect("OrderedFloat def contains inner type")
                                    .to_uppercase()
                                    .replace(">", "");
                                let ident = Ident::new(
                                    format!("Ordered{}Def", inner_type.trim()).as_str(),
                                    Span::call_site(),
                                );
                                quote! {
                                    #[rkyv(with = #ident)]
                                    pub #i: #type_,
                                }
                            } else {
                                quote! {pub #i: #type_,}
                            };

                            Ok::<_, syn::Error>(def)
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
