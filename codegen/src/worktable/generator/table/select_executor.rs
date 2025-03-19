use convert_case::{Case, Casing};
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;
use quote::ToTokens;
use syn::Type;

fn is_numeric_type(ty: &Type) -> bool {
    matches!(
        ty.to_token_stream().to_string().as_str(),
        "i8" | "i16" | "i32" | "i64" | "i128" | "u8" | "u16" | "u32" | "u64" | "u128"
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
            let variant_ident = Ident::new(
                &type_name.to_string().to_case(Case::Pascal),
                Span::call_site(),
            );
            let ty_ident = Ident::new(&type_name.to_string(), Span::call_site());
            quote! {
                #variant_ident(std::ops::RangeInclusive<#ty_ident>),
            }
        });

        let from_impls = unique_types.iter().map(|type_name| {
            let variant_ident = Ident::new(
                &type_name.to_string().to_case(Case::Pascal),
                Span::call_site(),
            );
            let type_ident = Ident::new(&type_name.to_string(), Span::call_site());

            quote! {
                impl From<std::ops::RangeInclusive<#type_ident>> for #column_range_type {
                    fn from(range: std::ops::RangeInclusive<#type_ident>) -> Self {
                        Self::#variant_ident(range)
                    }
                }
                impl From<std::ops::Range<#type_ident>> for #column_range_type {
                    fn from(range: std::ops::Range<#type_ident>) -> Self {
                        let end = if range.end > range.start {
                            range.end.saturating_sub(1)
                        } else {
                            range.end
                        };
                        Self::#variant_ident(range.start..=end)
                    }
                }
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

        let order_matches = self.columns.columns_map.keys().map(|column| {
            let col_lit = Literal::string(&column.to_string());
            let col_ident = Ident::new(&column.to_string(), Span::call_site());
            quote! {
                #col_lit => {
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
                let col_lit = Literal::string(column.to_string().as_str());
                let col_ident = Ident::new(&column.to_string(), Span::call_site());
                let variant_ident =
                    Ident::new(&ty.to_string().to_case(Case::Pascal), Span::call_site());
                quote! {
                    (#col_lit, #column_range_type::#variant_ident(range)) => {
                        Box::new(iter.filter(move |row| range.contains(&row.#col_ident)))
                            as Box<dyn DoubleEndedIterator<Item = #row_type>>
                    },
                }
            });

        quote! {
            impl<I> SelectQueryExecutor<#row_type, I, #column_range_type>
            for SelectQueryBuilder<#row_type, I, #column_range_type>
            where
                I: DoubleEndedIterator<Item = #row_type> + Sized,
            {

                fn where_by<F>(self, predicate: F) -> SelectQueryBuilder<#row_type, impl DoubleEndedIterator<Item = #row_type>  + Sized, #column_range_type>
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

                    if !self.params.range.is_empty() {
                        for (range, column) in &self.params.range {
                            iter = match (column.as_str(), range.clone().into()) {
                                #(#range_matches)*
                                _ => unreachable!(),
                            };
                        }
                    }

                    if !self.params.order.is_empty() {
                        let mut items: Vec<#row_type> = iter.collect();

                        items.sort_by(|a, b| {
                            for (order, col) in &self.params.order {
                                match col.as_str() {
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
