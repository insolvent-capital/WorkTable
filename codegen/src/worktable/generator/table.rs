use std::collections::HashMap;

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;
use crate::worktable::model::{GeneratorType, Index};

impl Generator {
    /// Generates type alias for new [`WorkTable`].
    ///
    /// [`WorkTable`]: worktable::WorkTable
    pub fn gen_table_def(&mut self) -> TokenStream {
        let name = &self.name;
        let ident = Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site());
        self.table_name = Some(ident.clone());

        let row_type = self.row_name.as_ref().unwrap();
        let pk_type = &self.pk.as_ref().unwrap().ident;
        let index_type = self.index_name.as_ref().unwrap();

        let get_next = match self.columns.generator_type {
            GeneratorType::Custom | GeneratorType::Autoincrement => {
                quote! {
                    pub fn get_next_pk(&self) -> #pk_type {
                        self.0.get_next_pk()
                    }
                }
            }
            GeneratorType::None => {
                quote! {}
            }
        };

        let iter_with = Self::gen_iter_with(row_type);
        let iter_with_async = Self::gen_iter_with_async(row_type);
        let select_executor = self.gen_select_executor();
        let select_result_executor = self.gen_select_result_executor();
        let table_name_lit = Literal::string(self.name.to_string().as_str());
        let page_const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_string().to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let inner_const_name = Ident::new(
            format!("{}_INNER_SIZE", name.to_string().to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let persist_type_part = if self.is_persist {
            quote! {
                , std::sync::Arc<DatabaseManager>
            }
        } else {
            quote! {}
        };
        let new_impl = if self.is_persist {
            quote! {
                 impl #ident {
                    fn new(manager:  std::sync::Arc<DatabaseManager>) -> Self {
                        let mut inner = WorkTable::default();
                        inner.table_name = #table_name_lit;
                        Self(inner, manager)
                    }
                }
            }
        } else {
            quote! {
                 impl Default for #ident {
                    fn default() -> Self {
                        let mut inner = WorkTable::default();
                        inner.table_name = #table_name_lit;
                        Self(inner)
                    }
                }
            }
        };
        let derive = if self.is_persist {
            quote! {
                 #[derive(Debug, PersistTable)]
            }
        } else {
            quote! {
                 #[derive(Debug)]
            }
        };

        let table = if let Some(page_size) = &self.config.as_ref().map(|c| c.page_size).flatten() {
            let page_size = Literal::usize_unsuffixed(*page_size as usize);
            quote! {
                const #page_const_name: usize = #page_size;
                const #inner_const_name: usize = #page_size - GENERAL_HEADER_SIZE;

                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #pk_type,
                        #index_type,
                        <#pk_type as TablePrimaryKey>::Generator,
                        #inner_const_name
                    >
                    #persist_type_part
                );
            }
        } else {
            quote! {
                const #page_const_name: usize = PAGE_SIZE;
                const #inner_const_name: usize = #page_const_name - GENERAL_HEADER_SIZE;

                #derive
                pub struct #ident(
                    WorkTable<
                        #row_type,
                        #pk_type,
                        #index_type
                    >
                    #persist_type_part
                );
            }
        };

        quote! {
            #table

            #new_impl

            impl #ident {
                pub fn name(&self) -> &'static str {
                    &self.0.table_name
                }

                pub fn select(&self, pk: #pk_type) -> Option<#row_type> {
                    self.0.select(pk)
                }

                pub fn insert(&self, row: #row_type) -> core::result::Result<#pk_type, WorkTableError> {
                    self.0.insert::<{ #row_type::ROW_SIZE }>(row)
                }

                pub async fn upsert(&self, row: #row_type) -> core::result::Result<(), WorkTableError> {
                    let pk = row.get_primary_key();
                    let need_to_update = {
                        let guard = Guard::new();
                        if let Some(_) = self.0.pk_map.peek(&pk, &guard) {
                            true
                        } else {
                            false
                        }
                    };
                    if need_to_update {
                        self.update(row).await?;
                    } else {
                        self.insert(row)?;
                    }
                    core::result::Result::Ok(())
                }

                #get_next

                #iter_with

                #iter_with_async
            }

            #select_executor

            #select_result_executor
        }
    }

    pub fn gen_select_result_executor(&self) -> TokenStream {
        let row_type = self.row_name.as_ref().unwrap();
        let name = &self.name;
        let ident = Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site());

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
                        println!("{:?} {:?}", q, col);
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

    pub fn gen_select_executor(&self) -> TokenStream {
        let row_type = self.row_name.as_ref().unwrap();
        let name = &self.name;
        let ident = Ident::new(format!("{}WorkTable", name).as_str(), Span::mixed_site());

        let columns = self.columns.columns_map.iter().map(|(name, _)| {
            let lit = Literal::string(name.to_string().as_str());
            if let Some(index) = self.columns.indexes.get(&name) {
                let idx_name = &index.name;
                if index.is_unique {
                    quote! {
                        #lit => {
                            let mut limit = q.params.limit.unwrap_or(usize::MAX);
                            let mut offset = q.params.offset.unwrap_or(0);
                            let guard = Guard::new();
                            let mut iter = self.0.indexes.#idx_name.iter(&guard);
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
                            let guard = Guard::new();
                            let mut iter = self.0.indexes.#idx_name.iter(&guard);
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
                        let mut iter = self.0.pk_map.iter(&guard);
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

                        Ok(rows)
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

    pub fn gen_table_index_impl(&mut self) -> syn::Result<TokenStream> {
        let fn_defs = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                if idx.is_unique {
                    Self::gen_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        self.row_name.clone().unwrap(),
                    )
                } else {
                    Self::gen_non_unique_index_fn(
                        i,
                        idx,
                        &self.columns.columns_map,
                        self.row_name.clone().unwrap(),
                    )
                }
            })
            .collect::<Result<Vec<_>, syn::Error>>()?;

        let table_ident = self.table_name.clone().unwrap();
        Ok(quote! {
            impl #table_ident {
                #(#fn_defs)*
            }
        })
    }

    fn gen_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(&i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> Option<#row_ident> {
                let guard = Guard::new();
                let link = self.0.indexes.#field_ident.peek(&by, &guard)?;
                self.0.data.select(*link).ok()
            }
        })
    }

    fn gen_non_unique_index_fn(
        i: &Ident,
        idx: &Index,
        columns_map: &HashMap<Ident, TokenStream>,
        row_ident: Ident,
    ) -> syn::Result<TokenStream> {
        let type_ = columns_map
            .get(&i)
            .ok_or(syn::Error::new(i.span(), "Row not found"))?;
        let fn_name = Ident::new(format!("select_by_{i}").as_str(), Span::mixed_site());
        let field_ident = &idx.name;

        Ok(quote! {
            pub fn #fn_name(&self, by: #type_) -> core::result::Result<SelectResult<#row_ident, Self>, WorkTableError> {
                let rows = {
                    let guard = Guard::new();
                    self.0.indexes.#field_ident
                        .peek(&by, &guard)
                        .ok_or(WorkTableError::NotFound)?
                        .iter()
                        .map(|l| *l.as_ref())
                        .collect::<Vec<_>>()
                }.iter().map(|link| {
                    self.0.data.select(*link).map_err(WorkTableError::PagesError)
                })
                .collect::<Result<Vec<_>, _>>()?;
                core::result::Result::Ok(SelectResult::<#row_ident, Self>::new(rows))
            }
        })
    }

    fn gen_iter_with(row: &Ident) -> TokenStream {
        quote! {
            pub fn iter_with<F: Fn(#row) -> core::result::Result<(), WorkTableError>>(&self, f: F) -> core::result::Result<(), WorkTableError> {
                let first = {
                    let guard = Guard::new();
                    self.0.pk_map.iter(&guard).next().map(|(k, v)| (k.clone(), *v))
                };
                let Some((mut k, link)) = first else {
                    return Ok(())
                };

                let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                f(data)?;

                let mut ind = false;
                while !ind {
                    let next = {
                        let guard = Guard::new();
                        let mut iter = self.0.pk_map.range(k.clone().., &guard);
                        let next = iter.next().map(|(k, v)| (k.clone(), *v)).filter(|(key, _)| key != &k);
                        if next.is_some() {
                            next
                        } else {
                            iter.next().map(|(k, v)| (k.clone(), *v))
                        }
                    };
                    if let Some((key, link)) = next {
                        let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                        f(data)?;
                        k = key
                    } else {
                        ind = true;
                    };
                }

                core::result::Result::Ok(())
            }
        }
    }

    fn gen_iter_with_async(row: &Ident) -> TokenStream {
        quote! {
            pub async fn iter_with_async<F: Fn(#row) -> Fut , Fut: std::future::Future<Output = core::result::Result<(), WorkTableError>>>(&self, f: F) ->core::result::Result<(), WorkTableError> {
                let first = {
                    let guard = Guard::new();
                    self.0.pk_map.iter(&guard).next().map(|(k, v)| (k.clone(), *v))
                };
                let Some((mut k, link)) = first else {
                    return Ok(())
                };

                let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                f(data).await?;

                let mut ind = false;
                while !ind {
                    let next = {
                        let guard = Guard::new();
                        let mut iter = self.0.pk_map.range(k.clone().., &guard);
                        let next = iter.next().map(|(k, v)| (k.clone(), *v)).filter(|(key, _)| key != &k);
                        if next.is_some() {
                            next
                        } else {
                            iter.next().map(|(k, v)| (k.clone(), *v))
                        }
                    };
                    if let Some((key, link)) = next {
                        let data = self.0.data.select(link).map_err(WorkTableError::PagesError)?;
                        f(data).await?;
                        k = key
                    } else {
                        ind = true;
                    };
                }

                core::result::Result::Ok(())
            }
        }
    }
}
