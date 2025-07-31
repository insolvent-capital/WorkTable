use crate::name_generator::WorktableNameGenerator;
use crate::worktable::generator::queries::r#type::map_to_uppercase;
use crate::worktable::generator::Generator;
use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

impl Generator {
    pub fn gen_secondary_index_cdc_impl_def(&mut self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let index_type_ident = name_generator.get_index_type_ident();
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();
        let available_types_ident = name_generator.get_available_type_ident();
        let available_index_ident = name_generator.get_available_indexes_ident();

        let save_row_cdc = self.gen_save_row_cdc_index_fn();
        let reinsert_row_cdc = self.gen_reinsert_row_cdc_index_fn();
        let delete_row_cdc = self.gen_delete_row_cdc_index_fn();
        let process_diff_cdc = self.gen_process_diff_cdc_index_fn();

        quote! {
            impl TableSecondaryIndexCdc<#row_type_ident, #available_types_ident, #events_ident, #available_index_ident> for #index_type_ident {
                #reinsert_row_cdc
                #save_row_cdc
                #delete_row_cdc

                #process_diff_cdc
            }
        }
    }

    fn gen_save_row_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();
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

                quote! {
                    let (exists, events) = self.#index_field_name.insert_cdc(row.#i.clone(), link);
                    if let Some(link) = exists {
                        self.#index_field_name.insert_cdc(row.#i, link);
                        return Err(IndexError::AlreadyExists {
                            at: #available_index_ident::#index_variant,
                            inserted_already: inserted_indexes.clone(),
                        });
                    }
                    let #index_field_name = events.into_iter().map(|ev| ev.into()).collect();
                }
            })
            .collect::<Vec<_>>();
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn save_row_cdc(&self, row: #row_type_ident, link: Link) -> Result<#events_ident, IndexError<#available_index_ident>> {
                let mut inserted_indexes: Vec<#available_index_ident> = vec![];

                #(#save_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    fn gen_reinsert_row_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let reinsert_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                let remove = if idx.is_unique {
                    quote! {
                        if row_new.#i != row_old.#i {
                            let (_, events) = TableIndexCdc::remove_cdc(&self.#index_field_name, row_old.#i.clone(), link_old);
                            #index_field_name.extend(events.into_iter().map(|ev| ev.into()).collect::<Vec<_>>());
                        }
                    }
                } else {
                    quote! {
                        let (_, events) = TableIndexCdc::remove_cdc(&self.#index_field_name, row_old.#i.clone(), link_old);
                        #index_field_name.extend(events.into_iter().map(|ev| ev.into()).collect::<Vec<_>>());
                    }
                };
                quote! {
                    let (_, events) = self.#index_field_name.insert_cdc(row_new.#i.clone(), link_new);
                    let mut #index_field_name: Vec<_> = events.into_iter().map(|ev| ev.into()).collect();
                    #remove
                }
            })
            .collect::<Vec<_>>();
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn reinsert_row_cdc(&self, row_old: #row_type_ident, link_old: Link, row_new: #row_type_ident, link_new: Link) -> eyre::Result<#events_ident> {
                #(#reinsert_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    fn gen_delete_row_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let row_type_ident = name_generator.get_row_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();
        let available_index_ident = name_generator.get_available_indexes_ident();

        let delete_rows = self
            .columns
            .indexes
            .iter()
            .map(|(i, idx)| {
                let index_field_name = &idx.name;
                quote! {
                    let (_, events) = TableIndexCdc::remove_cdc(&self.#index_field_name, row.#i, link);
                    let #index_field_name = events.into_iter().map(|ev| ev.into()).collect();
                }
            })
            .collect::<Vec<_>>();
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn delete_row_cdc(&self, row: #row_type_ident, link: Link) -> Result<#events_ident, IndexError<#available_index_ident>> {
                #(#delete_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }

    fn gen_process_diff_cdc_index_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_table_name(self.name.to_string());
        let avt_type_ident = name_generator.get_available_type_ident();
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let process_difference_rows = self.columns.indexes.iter().map(|(i, idx)| {
            let index_field_name = &idx.name;
            let diff_key = Literal::string(i.to_string().as_str());

            if let Some(t) = self.columns.columns_map.get(&idx.field) {
                let type_str = t.to_string();
                let variant_ident = Ident::new(&map_to_uppercase(&type_str), Span::mixed_site());

                let (new_value_expr, old_value_expr) = if type_str == "String" {
                    (quote! { new.to_string() }, quote! { old.to_string() })
                } else {
                    (quote! { *new }, quote! { *old })
                };

                quote! {
                    let #index_field_name = if let Some(diff) = difference.get(#diff_key) {
                        let mut events = vec![];
                        if let #avt_type_ident::#variant_ident(old) = &diff.old {
                            let key_old = #old_value_expr;
                            let (_, evs) = TableIndexCdc::remove_cdc(&self.#index_field_name, key_old, link);
                            events.extend_from_slice(evs.as_ref());
                        }

                        if let #avt_type_ident::#variant_ident(new) = &diff.new {
                            let key_new = #new_value_expr;
                            let (_, evs) = TableIndexCdc::insert_cdc(&self.#index_field_name, key_new, link);
                            events.extend_from_slice(evs.as_ref());
                        }
                        events
                    } else {
                        vec![]
                    };
                }
            } else {
                quote! {}
            }
        });
        let idents = self
            .columns
            .indexes
            .values()
            .map(|idx| &idx.name)
            .collect::<Vec<_>>();

        quote! {
            fn process_difference_cdc(
                &self,
                link: Link,
                difference: std::collections::HashMap<&str, Difference<#avt_type_ident>>
            ) -> core::result::Result<#events_ident, WorkTableError> {
                #(#process_difference_rows)*
                core::result::Result::Ok(
                    #events_ident {
                        #(#idents,)*
                    }
                )
            }
        }
    }
}
