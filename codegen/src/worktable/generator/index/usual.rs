use crate::name_generator::{is_float, WorktableNameGenerator};
use crate::worktable::generator::queries::r#type::map_to_uppercase;
use crate::worktable::generator::Generator;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

impl Generator {
    /// Generates implementation of `TableSecondaryIndex` trait for index.
    pub fn gen_secondary_index_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let index_type_ident = name_generator.get_index_type_ident();
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let save_row_fn = self.gen_save_row_index_fn();
        let reinsert_row_fn = self.gen_reinsert_row_index_fn();
        let delete_row_fn = self.gen_delete_row_index_fn();
        let process_difference_insert_fn = self.gen_process_difference_insert_index_fn();
        let process_difference_remove_fn = self.gen_process_difference_remove_index_fn();
        let delete_from_indexes = self.gen_index_delete_from_indexes_fn();

        quote! {
            impl TableSecondaryIndex<#row_type_ident, #avt_type_ident, #avt_index_ident> for #index_type_ident {
                #save_row_fn
                #reinsert_row_fn
                #delete_row_fn
                #process_difference_insert_fn
                #process_difference_remove_fn
                #delete_from_indexes
            }
        }
    }

    /// Generates `save_row` function of `TableSecondaryIndex` trait for index. It saves `Link` to all secondary
    /// indexes. Logic varies on index uniqueness. For unique index we can just insert `Link` in index, but for
    /// non-unique we need to get set from index first and then insert `Link` in set.
    fn gen_save_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let available_index_ident = name_generator.get_available_indexes_ident();

        let save_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let camel_case_name = index_field_name
                    .to_string()
                    .from_case(Case::Snake)
                    .to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();

                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                quote! {
                    if self.#index_field_name.insert_checked(#row.clone(), link).is_none() {
                        return Err(IndexError::AlreadyExists {
                            at: #available_index_ident::#index_variant,
                            inserted_already: inserted_indexes.clone(),
                        })
                    }
                    inserted_indexes.push(#available_index_ident::#index_variant);
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn save_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), IndexError<#available_index_ident>> {
                let mut inserted_indexes: Vec<#available_index_ident> = vec![];
                #(#save_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_reinsert_row_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let available_index_ident = name_generator.get_available_indexes_ident();

        let (insert_rows, remove_rows): (Vec<_>, Vec<_>) = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let camel_case_name = index_field_name
                    .to_string()
                    .from_case(Case::Snake)
                    .to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                let remove = if idx.is_unique {
                    quote! {
                        if val_new == val_old {
                            self.#index_field_name.insert(val_new.clone(), link_new);
                        } else {
                            TableIndex::remove(&self.#index_field_name, val_old, link_old);
                        }
                    }
                } else {
                    quote! {
                        self.#index_field_name.insert(val_new.clone(), link_new);
                        TableIndex::remove(&self.#index_field_name, val_old, link_old);
                    }
                };
                let insert = if idx.is_unique {
                    quote! {
                        let row = &row_new;
                        let val_new = #row.clone();
                        let row = &row_old;
                        let val_old = #row.clone();
                        if val_new != val_old {
                            if self.#index_field_name.insert_checked(val_new.clone(), link_new).is_none() {
                                return Err(IndexError::AlreadyExists {
                                    at: #available_index_ident::#index_variant,
                                    inserted_already: inserted_indexes.clone(),
                                })
                            }
                            inserted_indexes.push(#available_index_ident::#index_variant);
                        }
                    }
                } else {
                    quote! {}
                };
                let remove = quote! {
                    let row = &row_new;
                    let val_new = #row.clone();
                    let row = &row_old;
                    let val_old = #row.clone();
                    #remove
                };
                (insert, remove)
            })
            .unzip();

        quote! {
            fn reinsert_row(&self,
                row_old: #row_type_ident,
                link_old: Link,
                row_new: #row_type_ident,
                link_new: Link
            ) -> core::result::Result<(), IndexError<#available_index_ident>>
            {
                let mut inserted_indexes: Vec<#available_index_ident> = vec![];
                #(#insert_rows)*
                #(#remove_rows)*
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
        let available_index_ident = name_generator.get_available_indexes_ident();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                if idx.is_unique {
                    quote! {
                        self.#index_field_name.remove(&#row);
                    }
                } else {
                    quote! {
                        self.#index_field_name.remove(&#row, &link);
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            fn delete_row(&self, row: #row_type_ident, link: Link) -> core::result::Result<(), IndexError<#available_index_ident>> {
                #(#delete_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `process_difference_remove` function of `TableIndex` trait for index. It updates `Link` for all secondary indexes.
    /// Uses HashMap<&str, Difference<AvaialableTypes>> for storing all changes
    fn gen_process_difference_remove_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let process_difference_remove_rows = self.columns.indexes.iter().map(|(i, idx)| {
            let index_field_name = &idx.name;
            let diff_key = Literal::string(i.to_string().as_str());

            if let Some(t) = self.columns.columns_map.get(&idx.field) {
                let type_str = t.to_string();
                let variant_ident = Ident::new(&map_to_uppercase(&type_str), Span::mixed_site());

                let old_value_expr = if type_str == "String" {
                    quote! { old.to_string() }
                } else if is_float(type_str.as_str()) {
                    quote! { OrderedFloat(*old) }
                } else {
                    quote! { *old }
                };

                quote! {
                    if let Some(diff) = difference.get(#diff_key) {
                        if let #avt_type_ident::#variant_ident(old) = &diff.old {
                            let key_old = #old_value_expr;
                            TableIndex::remove(&self.#index_field_name, key_old, link);
                        }
                    }
                }
            } else {
                quote! {}
            }
        });

        quote! {
            fn process_difference_remove(
                &self,
                link: Link,
                difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>
            ) -> core::result::Result<(), IndexError<#avt_index_ident>> {
                #(#process_difference_remove_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    /// Generates `process_difference_insert` function of `TableIndex` trait for index. It updates `Link` for all secondary indexes.
    /// Uses HashMap<&str, Difference<AvaialableTypes>> for storing all changes
    fn gen_process_difference_insert_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let process_difference_insert_rows = self.columns.indexes.iter().map(|(i, idx)| {
            let index_field_name = &idx.name;
            let diff_key = Literal::string(i.to_string().as_str());

            if let Some(t) = self.columns.columns_map.get(&idx.field) {
                let type_str = t.to_string();
                let variant_ident = Ident::new(&map_to_uppercase(&type_str), Span::mixed_site());
                let camel_case_name = index_field_name
                    .to_string()
                    .from_case(Case::Snake)
                    .to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();

                let new_value_expr = if type_str == "String" {
                    quote! { new.to_string() }
                } else if is_float(type_str.as_str()) {
                    quote! { OrderedFloat(*new) }
                } else {
                    quote! { *new }
                };

                quote! {
                    if let Some(diff) = difference.get(#diff_key) {
                        if let #avt_type_ident::#variant_ident(new) = &diff.new {
                            let key_new = #new_value_expr;
                            if TableIndex::insert_checked(&self.#index_field_name, key_new, link).is_none() {
                                return Err(IndexError::AlreadyExists {
                                    at: #avt_index_ident::#index_variant,
                                    inserted_already: inserted_indexes.clone(),
                                })
                            }
                            inserted_indexes.push(#avt_index_ident::#index_variant);
                        }
                    }
                }
            } else {
                quote! {}
            }
        });

        quote! {
            fn process_difference_insert(
                &self,
                link: Link,
                difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>
            ) -> core::result::Result<(), IndexError<#avt_index_ident>> {
                let mut inserted_indexes: Vec<#avt_index_ident> = vec![];
                #(#process_difference_insert_rows)*
                core::result::Result::Ok(())
            }
        }
    }

    fn gen_index_delete_from_indexes_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_index_ident = name_generator.get_available_indexes_ident();
        let row_type_ident = name_generator.get_row_type_ident();

        let matches = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let camel_case_name = index_field_name
                    .to_string()
                    .from_case(Case::Snake)
                    .to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                let row = if is_float(
                    self.columns
                        .columns_map
                        .get(i)
                        .unwrap()
                        .to_string()
                        .as_str(),
                ) {
                    quote! {
                        OrderedFloat(row.#i)
                    }
                } else {
                    quote! {
                        row.#i
                    }
                };
                let delete = if idx.is_unique {
                    quote! {
                        self.#index_field_name.remove(&#row);
                    }
                } else {
                    quote! {
                        self.#index_field_name.remove(&#row, &link);
                    }
                };

                quote! {
                    #avt_index_ident::#index_variant => {
                        #delete
                    },
                }
            })
            .collect::<Vec<_>>();

        let inner = if matches.is_empty() {
            quote! {}
        } else {
            quote! {
                for index in indexes {
                    match index {
                        #(#matches)*
                    }
                }
            }
        };

        quote! {
            fn delete_from_indexes(
                &self,
                row: #row_type_ident,
                link: Link,
                indexes: Vec<#avt_index_ident>,
            ) -> core::result::Result<(), IndexError<#avt_index_ident>> {
                #inner
                core::result::Result::Ok(())
            }
        }
    }
}
