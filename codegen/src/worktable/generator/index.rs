use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    /// Generates index type and it's impls.
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_impl_def();

        quote! {
            #type_def

            #impl_def
        }
    }

    /// Generates table's secondary index struct definition. It has fields with index names and types varying on index
    /// uniqueness. For unique index it's `TreeIndex<T, Link`, for non-unique `TreeIndex<T, Arc<LockFreeSet<Link>>>`.
    /// Index also derives `PersistIndex` macro.
    fn gen_type_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let ident = name_generator.get_index_type_ident();
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

        quote! {
            #[derive(Debug, Default, PersistIndex)]
            pub struct #ident {
                #(#index_rows),*
            }
        }
    }

    /// Generates implementation of `TableIndex` trait for index.
    fn gen_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let index_type_ident = name_generator.get_index_type_ident();

        let save_row_fn = self.gen_save_row_index_fn();
        let delete_row_fn = self.gen_delete_row_index_fn();

        quote! {
            impl TableSecondaryIndex<#row_type_ident> for #index_type_ident {
                #save_row_fn
                #delete_row_fn
            }
        }
    }

    /// Generates `save_row` function of `TableIndex` trait for index. It saves `Link` to all secondary indexes. Logic
    /// varies on index uniqueness. For unique index we can just insert `Link` in index, but for non-unique we need to
    /// get set from index first and then insert `Link` in set.
    fn gen_save_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

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
                            set.insert(link).expect("`Link` should not be already in set");
                            TableIndex::insert(&self.#index_field_name, row.#i, std::sync::Arc::new(set))
                                .map_err(|_| WorkTableError::AlreadyExists)?;
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn save_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#save_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `delete_row` function of `TableIndex` trait for index. It removes `Link` from all secondary indexes.
    /// Logic varies on index uniqueness. For unique index we can just delete `Link` from index, but for non-unique we
    /// need to get set from index first and then delete `Link` from set.
    fn gen_delete_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

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

        quote! {
            fn delete_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), WorkTableError> {
                #(#delete_rows)*
                core::result::Result::Ok(())
            }
        }
    }
}

// TODO: tests...
