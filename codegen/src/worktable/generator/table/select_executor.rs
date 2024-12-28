use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_table_select_executor_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();

        let columns = self.columns.columns_map.iter().map(|(name, _)| {
            let lit = Literal::string(name.to_string().as_str());
            if let Some(index) = self.columns.indexes.get(&name) {
                let idx_name = &index.name;
                if index.is_unique {
                    quote! {
                        #lit => {
                            let mut limit = q.params.limit.unwrap_or(usize::MAX);
                            let mut offset = q.params.offset.unwrap_or(0);
                            let mut iter = TableIndex::iter(&self.0.indexes.#idx_name);
                            let mut rows = vec![];

                            while let Some((_, l)) = iter.next() {
                                if q.params.orders.len() < 2 {
                                    if offset != 0 {
                                        offset -= 1;
                                        continue;
                                    }
                                }
                                let next = self.0.data.select(*l).map_err(WorkTableError::PagesError)?;
                                rows.push(next);
                                if q.params.orders.len() < 2 {
                                    limit -= 1;
                                    if limit == 0 {
                                        break
                                    }
                                }
                            }

                            rows
                        },
                    }
                } else {
                    quote! {
                        #lit => {
                            let mut limit = q.params.limit.unwrap_or(usize::MAX);
                            let mut offset = q.params.offset.unwrap_or(0);
                            let mut iter = TableIndex::iter(&self.0.indexes.#idx_name);
                            let mut rows = vec![];

                            while let Some((_, links)) = iter.next() {
                                for l in links.iter() {
                                    if q.params.orders.len() < 2 {
                                        if offset != 0 {
                                            offset -= 1;
                                            continue;
                                        }
                                    }
                                    let next = self.0.data.select(*l.as_ref()).map_err(WorkTableError::PagesError)?;
                                    rows.push(next);
                                    if q.params.orders.len() < 2 {
                                        limit -= 1;
                                        if limit == 0 {
                                            break
                                        }
                                    }
                                }
                                if limit == 0 {
                                    break
                                }
                            }

                            rows
                        }
                    }
                }
            } else {
                // TODO: Add support for non-indexed columns
                quote! {
                    #lit => todo!(),
                }
            }
        }).collect::<Vec<_>>();

        quote! {
            impl SelectQueryExecutor<'_, #row_type> for #ident {
                fn execute(&self, mut q: SelectQueryBuilder<#row_type, Self>) -> Result<Vec<#row_type>, WorkTableError> {
                    if q.params.orders.is_empty() {
                        let mut limit = q.params.limit.unwrap_or(usize::MAX);
                        let mut offset = q.params.offset.unwrap_or(0);
                        let guard = Guard::new();
                        let mut iter = TableIndex::iter(&self.0.pk_map);
                        let mut rows = vec![];

                        while let Some((_, l)) = iter.next() {
                            if offset != 0 {
                                offset -= 1;
                                continue;
                            }
                            let next = self.0.data.select(*l).map_err(WorkTableError::PagesError)?;
                            rows.push(next);
                            if q.params.orders.len() < 2 {
                                limit -= 1;
                                if limit == 0 {
                                    break
                                }
                            }
                        }

                        core::result::Result::Ok(rows)
                    } else {
                        let (order, column) = q.params.orders.pop_front().unwrap();
                        q.params.orders.push_front((order, column.clone()));
                        let rows = match column.as_str() {
                            #(#columns)*
                            _ => unreachable!()
                        };
                        core::result::Result::Ok(SelectResult::<_, Self>::new(rows).with_params(q.params).execute())
                    }
                }
            }
        }
    }

    pub fn gen_table_select_result_executor_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_work_table_ident();
        let row_type = name_generator.get_row_type_ident();

        let columns = self
            .columns
            .columns_map
            .iter()
            .map(|(name, _)| {
                let lit = Literal::string(name.to_string().as_str());
                quote! {
                    #lit => {
                        sort = Box::new(move |left, right| {match sort(left, right) {
                            std::cmp::Ordering::Equal => {
                                match q {
                                    Order::Asc => {
                                        (&left.#name).partial_cmp(&right.#name).unwrap()
                                    },
                                    Order::Desc => {
                                        (&right.#name).partial_cmp(&left.#name).unwrap()
                                    }
                                }
                            },
                            std::cmp::Ordering::Less => {
                                std::cmp::Ordering::Less
                            },
                            std::cmp::Ordering::Greater => {
                                std::cmp::Ordering::Greater
                            },
                        }});
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            impl SelectResultExecutor<#row_type> for #ident {
                fn execute(mut q: SelectResult<#row_type, Self>) -> Vec<#row_type> {
                    let mut sort: Box<dyn Fn(&#row_type, &#row_type) ->  std::cmp::Ordering> = Box::new(|left: &#row_type, right: &#row_type| { std::cmp::Ordering::Equal });
                    while let Some((q, col)) = q.params.orders.pop_front() {
                        match col.as_str() {
                            #(#columns)*
                            _ => unreachable!()
                        }
                    }
                    q.vals.sort_by(sort);

                    let offset = q.params.offset.unwrap_or(0);
                    let mut vals = q.vals.as_slice()[offset..].to_vec();
                    if let Some(l) = q.params.limit {
                        vals.truncate(l);
                        vals
                    } else {
                        vals
                    }
                }
            }
        }
    }
}
