use convert_case::{Case, Casing};
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use quote::ToTokens;
use syn::Type;

const RANGE_VARIANTS: &[&str] = &["", "Inclusive", "From", "To", "ToInclusive"];

fn is_numeric_type(ty: &Type) -> bool {
    matches!(
        ty.to_token_stream().to_string().as_str(),
        "i8" | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "f32"
            | "f64"
    )
}

impl Generator {
    pub fn gen_table_column_range_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let column_range_type = name_generator.get_column_range_type_ident();

        let unique_types: std::collections::HashSet<String> = self
            .columns
            .columns_map
            .values()
            .map(|ty| ty.to_token_stream().to_string())
            .filter(|ty| is_numeric_type(&syn::parse_str::<Type>(ty).unwrap()))
            .map(|ty| ty.to_string())
            .collect();

        let column_range_variants = unique_types.iter().map(|type_name| {
            let ty_ident = Ident::new(&type_name.to_string(), Span::call_site());
            let variants: Vec<_> = RANGE_VARIANTS
                .iter()
                .map(|variant| {
                    let variant_ident = Ident::new(
                        &format!("{}{}", type_name.to_string().to_case(Case::Pascal), variant),
                        Span::call_site(),
                    );
                    let range_ident = Ident::new(&format!("Range{variant}"), Span::call_site());
                    quote! {
                        #variant_ident(std::ops::#range_ident<#ty_ident>),
                    }
                })
                .collect();

            quote! {
                #(#variants)*
            }
        });

        let from_impls = unique_types.iter().map(|type_name| {
            let ty_ident = Ident::new(&type_name.to_string(), Span::call_site());
            let variants: Vec<_> = RANGE_VARIANTS
                .iter()
                .map(|variant| {
                    let variant_ident = Ident::new(
                        &format!("{}{}", type_name.to_string().to_case(Case::Pascal), variant),
                        Span::call_site(),
                    );
                    let range_ident = Ident::new(&format!("Range{variant}"), Span::call_site());
                    quote! {
                        impl From<std::ops::#range_ident<#ty_ident>> for #column_range_type {
                            fn from(range: std::ops::#range_ident<#ty_ident>) -> Self {
                                Self::#variant_ident(range)
                            }
                        }
                    }
                })
                .collect();

            quote! {
                #(#variants)*
            }
        });

        quote! {
            #[derive(Debug, Clone)]
            pub enum #column_range_type {
                #(#column_range_variants)*
            }

            #(#from_impls)*
        }
    }

    pub fn gen_table_select_query_executor_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type = name_generator.get_row_type_ident();
        let column_range_type = name_generator.get_column_range_type_ident();
        let row_fields_ident = name_generator.get_row_fields_enum_ident();

        let order_matches = self.columns.columns_map.keys().map(|column| {
            let column_variant = Ident::new(&column.to_string().to_case(Case::Pascal), Span::mixed_site());
            let col_ident = Ident::new(&column.to_string(), Span::call_site());
            quote! {
                #row_fields_ident::#column_variant => {
                    let cmp = a.#col_ident.partial_cmp(&b.#col_ident).unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return match order {
                            Order::Asc => cmp,
                            Order::Desc => cmp.reverse(),
                        };
                    }
                }
            }
        });

        let range_matches = self
            .columns
            .columns_map
            .iter()
            .filter(|(_, ty)| {
                is_numeric_type(&syn::parse_str::<Type>(&ty.to_token_stream().to_string()).unwrap())
            })
            .map(|(column, ty)| {
                let variants: Vec<_> = RANGE_VARIANTS
                    .iter()
                    .map(|v| {
                        let column_variant = Ident::new(&column.to_string().to_case(Case::Pascal), Span::mixed_site());
                        let col_ident = Ident::new(&column.to_string(), Span::call_site());
                        let variant_ident = Ident::new(
                            &format!("{}{}", ty.to_string().to_case(Case::Pascal), v),
                            Span::call_site(),
                        );
                        quote! {
                            (#row_fields_ident::#column_variant, #column_range_type::#variant_ident(range)) => {
                                Box::new(iter.filter(move |row| range.contains(&row.#col_ident)))
                                    as Box<dyn DoubleEndedIterator<Item = #row_type>>
                            },
                        }
                    })
                    .collect();

                quote! {
                    #(#variants)*
                }
            }).collect::<Vec<_>>();

        let range = if range_matches.is_empty() {
            quote! {}
        } else {
            quote! {
                if !self.params.range.is_empty() {
                for (range, column) in &self.params.range {
                    iter = match (column, range.clone().into()) {
                        #(#range_matches)*
                        _ => unreachable!(),
                    };
                }
            }
            }
        };

        quote! {
            impl<I> SelectQueryExecutor<#row_type, I, #column_range_type, #row_fields_ident>
            for SelectQueryBuilder<#row_type, I, #column_range_type, #row_fields_ident>
            where
                I: DoubleEndedIterator<Item = #row_type> + Sized,
            {

                fn where_by<F>(self, predicate: F) -> SelectQueryBuilder<#row_type,
                                                                         impl DoubleEndedIterator<Item = #row_type>  + Sized,
                                                                         #column_range_type,
                                                                         #row_fields_ident>
                where
                    F: FnMut(&#row_type) -> bool,
                {
                    SelectQueryBuilder {
                        params: self.params,
                        iter: self.iter.filter(predicate),
                    }
                }

                fn execute(self) -> Result<Vec<#row_type>, WorkTableError> {
                    let mut iter: Box<dyn DoubleEndedIterator<Item = #row_type>> = Box::new(self.iter);

                    #range

                    if !self.params.order.is_empty() {
                        let mut items: Vec<#row_type> = iter.collect();

                        items.sort_by(|a, b| {
                            for (order, col) in &self.params.order {
                                match col {
                                    #(#order_matches)*
                                    _ => continue,
                                }
                            }
                            std::cmp::Ordering::Equal
                        });

                        iter = Box::new(items.into_iter());
                    }

                    let iter_result: Box<dyn Iterator<Item = #row_type>> = if let Some(offset) = self.params.offset {
                        Box::new(iter.skip(offset))
                    } else {
                        Box::new(iter)
                    };

                    let iter_result: Box<dyn Iterator<Item = #row_type>> = if let Some(limit) = self.params.limit {
                        Box::new(iter_result.take(limit))
                    } else {
                        Box::new(iter_result)
                    };

                    Ok(iter_result.collect())
                }
            }
        }
    }
}
