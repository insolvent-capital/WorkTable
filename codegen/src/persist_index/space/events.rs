use convert_case::{Case, Casing};
use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_index::generator::Generator;

impl Generator {
    pub fn gen_space_secondary_index_events_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                quote! {
                    #i: Vec<IndexChangeEvent<
                        IndexPair<#t, Link>
                    >>,
                }
            })
            .collect();

        quote! {
            #[derive(Clone, Debug, Default)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }

    pub fn gen_space_secondary_index_events_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let extend_fn = self.gen_space_secondary_index_events_extend_fn();
        let remove_fn = self.gen_space_secondary_index_events_remove_fn();
        let last_evs_fn = self.gen_space_secondary_index_events_last_evs_fn();
        let first_evs = self.gen_space_secondary_index_events_first_evs_fn();
        let iter_event_ids_fn = self.gen_space_secondary_index_events_iter_event_ids_fn();
        let contains_event_fn = self.gen_space_secondary_index_events_contains_event_fn();
        let split_is_first_fn = self.gen_space_secondary_index_events_is_first_split_fn();
        let sort_fn = self.gen_space_secondary_index_events_sort_fn();
        let validate_fn = self.gen_space_secondary_index_events_validate_fn();
        let is_empty_fn = self.gen_space_secondary_index_events_is_empty_fn();
        let is_unit_fn = self.gen_space_secondary_index_events_is_unit_fn();

        quote! {
            impl TableSecondaryIndexEventsOps<#avt_index_ident> for #ident {
                #extend_fn
                #remove_fn
                #last_evs_fn
                #first_evs
                #iter_event_ids_fn
                #contains_event_fn
                #split_is_first_fn
                #sort_fn
                #validate_fn
                #is_empty_fn
                #is_unit_fn
            }
        }
    }

    fn gen_space_secondary_index_events_sort_fn(&self) -> TokenStream {
        let fields_sort: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    self.#i.sort_by(|ev1, ev2| ev1.id().cmp(&ev2.id()));
                }
            })
            .collect();

        quote! {
            fn sort(&mut self) {
                    #(#fields_sort)*
                }
        }
    }

    fn gen_space_secondary_index_events_first_evs_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let fields_first: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let camel_case_name = i.to_string().from_case(Case::Snake).to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                quote! {
                    let first = self.#i.first().map(|ev| ev.id());
                    map.insert(#avt_index_ident::#index_variant, first);
                }
            })
            .collect();

        quote! {
            fn first_evs(&self) -> std::collections::HashMap<#avt_index_ident, Option<IndexChangeEventId>> {
                    let mut map = std::collections::HashMap::new();
                    #(#fields_first)*
                    map
                }
        }
    }

    fn gen_space_secondary_index_events_last_evs_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let fields_last: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let camel_case_name = i.to_string().from_case(Case::Snake).to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                quote! {
                    let last = self.#i.last().map(|ev| ev.id());
                    map.insert(#avt_index_ident::#index_variant, last);
                }
            })
            .collect();

        quote! {
            fn last_evs(&self) -> std::collections::HashMap<#avt_index_ident, Option<IndexChangeEventId>> {
                    let mut map = std::collections::HashMap::new();
                    #(#fields_last)*
                    map
                }
        }
    }

    fn gen_space_secondary_index_events_extend_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields_extend: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    self.#i.extend(another.#i);
                }
            })
            .collect();

        quote! {
            fn extend(&mut self, another: #ident) {
                    #(#fields_extend)*
                }
        }
    }

    fn gen_space_secondary_index_events_remove_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields_remove: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    for ev in &another.#i {
                        if let Ok(pos) = self.#i
                            .binary_search_by(|inner_ev| inner_ev.id().cmp(&ev.id())) {
                            self.#i.remove(pos);
                        }

                    }
                }
            })
            .collect();

        quote! {
            fn remove(&mut self, another: &#ident) {
                    #(#fields_remove)*
                }
        }
    }

    fn gen_space_secondary_index_events_iter_event_ids_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let fields_iter: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let camel_case_name = i.to_string().from_case(Case::Snake).to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                quote! {
                    self.#i.iter().map(|ev| (#avt_index_ident::#index_variant, ev.id())).collect::<Vec<_>>()
                }
            })
            .collect();

        quote! {
            fn iter_event_ids(&self) -> impl Iterator<Item = (#avt_index_ident, IndexChangeEventId)> {
                <std::vec::IntoIter<Vec<(#avt_index_ident, IndexChangeEventId)>> as Iterator>::flatten(
                    vec![
                    #(#fields_iter),*
                ]
                    .into_iter()
                    )
                }
        }
    }

    fn gen_space_secondary_index_events_is_first_split_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let fields_matches: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let camel_case_name = i.to_string().from_case(Case::Snake).to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                quote! {
                    #avt_index_ident::#index_variant => {
                        if let Some(first) = self.#i.first() {
                            match first {
                                IndexChangeEvent::SplitNode {..} => {
                                    true
                                }
                                _ => {
                                    false
                                }
                            }
                        } else {
                            false
                        }
                    }
                }
            })
            .collect();

        if fields_matches.is_empty() {
            quote! {
                fn is_first_ev_is_split(&self, _index: #avt_index_ident) -> bool {
                    false
                }
            }
        } else {
            quote! {
                fn is_first_ev_is_split(&self, index: #avt_index_ident) -> bool {
                    match index {
                        #(#fields_matches),*
                    }
                }
            }
        }
    }

    fn gen_space_secondary_index_events_contains_event_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let avt_index_ident = name_generator.get_available_indexes_ident();

        let fields_matches: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let camel_case_name = i.to_string().from_case(Case::Snake).to_case(Case::Pascal);
                let index_variant: TokenStream = camel_case_name.parse().unwrap();
                quote! {
                    #avt_index_ident::#index_variant => {
                        self.#i.iter().map(|ev| ev.id()).any(|ev_id| ev_id == id)
                    }
                }
            })
            .collect();

        if fields_matches.is_empty() {
            quote! {
                fn contains_event(&self, index: #avt_index_ident, id: IndexChangeEventId) -> bool {
                    false
                }
            }
        } else {
            quote! {
                fn contains_event(&self, index: #avt_index_ident, id: IndexChangeEventId) -> bool {
                    match index {
                        #(#fields_matches),*
                    }
                }
            }
        }
    }

    fn gen_space_secondary_index_events_validate_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields_validate: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    let #i = validate_events(&mut self.#i);
                }
            })
            .collect();
        let fields_init: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    #i,
                }
            })
            .collect();

        quote! {
            fn validate(&mut self) -> #ident {
                    #(#fields_validate)*
                    Self {
                        #(#fields_init)*
                    }
                }
        }
    }

    fn gen_space_secondary_index_events_is_empty_fn(&self) -> TokenStream {
        let is_empty: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    self.#i.is_empty()
                }
            })
            .collect();
        if is_empty.is_empty() {
            quote! {
                    fn is_empty(&self) -> bool {
                        true
                    }
            }
        } else {
            quote! {
                    fn is_empty(&self) -> bool {
                        #(#is_empty) &&*
                    }
            }
        }
    }

    fn gen_space_secondary_index_events_is_unit_fn(&self) -> TokenStream {
        let is_unit = if self.field_types.is_empty() {
            quote! {
                true
            }
        } else {
            quote! {
                false
            }
        };

        quote! {
            fn is_unit() -> bool {
                    #is_unit
                }
        }
    }
}
