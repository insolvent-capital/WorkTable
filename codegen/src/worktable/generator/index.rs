use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

use crate::worktable::generator::Generator;

impl Generator {
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_impl_def();

        quote! {
            #type_def
            #impl_def
        }
    }

    fn gen_type_def(&mut self) -> TokenStream {
        let name = &self.name;
        let index_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_type = &idx.index_type;
                let t = self.columns.columns_map.get(&i);
                let i = &idx.name;

                if idx.is_unique {
                    quote! {#i: #index_type<#t, Link>}
                } else {
                    quote! {#i: #index_type<#t, std::sync::Arc<LockFreeSet<Link>>>}
                }
            })
            .collect::<Vec<_>>();

        let ident = Ident::new(format!("{name}Index").as_str(), Span::mixed_site());
        self.index_name = Some(ident.clone());
        let struct_def = quote! {pub struct #ident};
        quote! {
            #[derive(Debug, Default, PersistIndex)]
            #struct_def {
                #(#index_rows),*
            }
        }
    }

    fn gen_impl_def(&mut self) -> TokenStream {
        let save_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                if idx.is_unique {
                    quote! {
                        TableIndex::insert(&self.#index_field_name, row.#i, link)
                            .map_err(|_| WorkTableError::AlreadyExists)?;
                    }
                } else {
                    quote! {
                        if let Some(set) = TableIndex::peek(&self.#index_field_name, &row.#i) {
                            set.insert(link).expect("is ok");
                        } else {
                            let set = LockFreeSet::new();
                            set.insert(link).expect("is ok");
                            TableIndex::insert(&self.#index_field_name, row.#i, std::sync::Arc::new(set))
                                .map_err(|_| WorkTableError::AlreadyExists)?;
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                if idx.is_unique {
                    quote! {
                        TableIndex::remove(&self.#index_field_name, &row.#i);
                    }
                } else {
                    quote! {
                        if let Some(set) = TableIndex::peek(&self.#index_field_name, &row.#i) {
                            set.remove(&link);
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let row_type_name = self.row_name.as_ref().unwrap();
        let index_type_name = self.index_name.as_ref().unwrap();

        quote! {
            impl TableSecondaryIndex<#row_type_name> for #index_type_name {
                fn save_row(&self, row: #row_type_name, link: Link) -> core::result::Result<(), WorkTableError> {
                    #(#save_rows)*

                    core::result::Result::Ok(())
                }

                fn delete_row(&self, row: #row_type_name, link: Link) -> core::result::Result<(), WorkTableError> {
                    #(#delete_rows)*

                    core::result::Result::Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use proc_macro2::{Ident, Span, TokenStream};
    use quote::quote;

    use crate::worktable::generator::Generator;
    use crate::worktable::Parser;

    #[test]
    fn test_type_def() {
        let tokens = TokenStream::from(quote! {
            columns: {
                id: i64 primary_key,
                test: u64,
            },
            indexes: {
                test_idx: test,
            }
        });
        let mut parser = Parser::new(tokens);

        let mut columns = parser.parse_columns().unwrap();
        let idx = parser.parse_indexes().unwrap();
        columns.indexes = idx;

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, false, columns);

        let res = generator.gen_type_def();

        assert_eq!(
            generator.index_name.unwrap().to_string(),
            "TestIndex".to_string()
        );
        assert_eq!(res.to_string(), "# [derive (Debug , Default , Clone)] pub struct TestIndex { test_idx : TreeIndex < u64 , Link > }")
    }

    #[test]
    fn test_impl_def() {
        let tokens = TokenStream::from(quote! {
            columns: {
                id: i64 primary_key,
                test: u64,
            },
            indexes: {
                test_idx: test,
            }
        });
        let mut parser = Parser::new(tokens);

        let mut columns = parser.parse_columns().unwrap();
        let idx = parser.parse_indexes().unwrap();
        columns.indexes = idx;

        let ident = Ident::new("Test", Span::call_site());
        let mut generator = Generator::new(ident, false, columns);
        generator.gen_type_def();
        generator.gen_pk_def();
        generator.gen_row_def();

        let res = generator.gen_impl_def();

        assert_eq!(res.to_string(), "impl TableIndex < TestRow > for TestIndex { fn save_row (& self , row : TestRow , link : Link) { self . test_idx . insert (row . test , link) ; } }")
    }
}
