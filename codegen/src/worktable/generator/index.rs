use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::Generator;

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

impl Generator {
    /// Generates index type and it's impls.
    pub fn gen_index_def(&mut self) -> TokenStream {
        let type_def = self.gen_type_def();
        let impl_def = self.gen_impl_def();

        println!("{}", self.gen_impl_def());

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
                let t = self.columns.columns_map.get(&i);
                let i = &idx.name;

                if idx.is_unique {
                    quote! {#i: IndexMap<#t, Link>}
                } else {
                    quote! {#i: IndexMultiMap<#t, Link>}
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

    /// Generates implementation of `TableSecondaryIndex` trait for index.
    fn gen_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let index_type_ident = name_generator.get_index_type_ident();
        let avt_type_ident = name_generator.get_available_type_ident();

        let save_row_fn = self.gen_save_row_index_fn();
        let delete_row_fn = self.gen_delete_row_index_fn();
        let process_difference_fn = self.gen_process_difference_index_fn();

        quote! {
            impl TableSecondaryIndex<#row_type_ident, #avt_type_ident> for #index_type_ident {
                #save_row_fn
                #delete_row_fn
                #process_difference_fn
            }
        }
    }

    /// Generates `save_row` function of `TableSecondaryIndex` trait for index. It saves `Link` to all secondary
    /// indexes. Logic varies on index uniqueness. For unique index we can just insert `Link` in index, but for
    /// non-unique we need to get set from index first and then insert `Link` in set.
    fn gen_save_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();

        let save_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                quote! {
                    self.#index_field_name.insert(row.#i, link)
                    .map_or(Ok(()), |_| Err(WorkTableError::AlreadyExists))?;
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
                let lit = Literal::string(index_field_name.to_string().as_str());
                if idx.is_unique {
                    quote! {
                        println!("{} remove", #lit);
                        self.#index_field_name.remove(&row.#i);
                        println!("{} removed", #lit);
                    }
                } else {
                    quote! {
                        println!("{} remove", #lit);
                        self.#index_field_name.remove(&row.#i, &link);
                        println!("{} removed", #lit);
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

    fn gen_process_difference_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();

        let process_difference_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let diff_key = Literal::string(i.to_string().as_str());

                let match_arms: Vec<_> = self
                    .columns
                    .columns_map
                    .get(&idx.field)
                    .map(|t| {
                        let type_str = t.to_string();
                        let variant_ident =
                            Ident::new(&type_str.to_uppercase(), Span::mixed_site());

                        let new_value_expr = if type_str == "String" {
                            quote! { new.to_string() }
                        } else {
                            quote! { *new }
                        };

                        let old_value_expr = if type_str == "String" {
                            quote! { old.to_string() }
                        } else {
                            quote! { *old }
                        };

                        quote! {
                            if let Some(diff) = difference.get(#diff_key) {
                                match &diff.old {
                                   #avt_type_ident::#variant_ident(old) => {
                                    let key_old = #old_value_expr;
                                    TableIndex::remove(&self.#index_field_name, key_old, link);
                                   }
                                    _ => {}
                                }

                                match &diff.new {
                                    #avt_type_ident::#variant_ident(new) => {
                                         let key_new = #new_value_expr;
                                         TableIndex::insert(&self.#index_field_name, key_new, link);
                                     }
                                     _ => {}
                                }
                            }
                        }
                    })
                    .into_iter()
                    .collect();

                quote! {
                        #(#match_arms)*
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn process_difference(&self, link: Link, difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>) -> core::result::Result<(), WorkTableError> {
                #(#process_difference_rows)*
                core::result::Result::Ok(())
            }
        }
    }
}

// TODO: tests...
