use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_index::generator::Generator;

impl Generator {
    pub fn gen_space_index(&self) -> TokenStream {
        let secondary_index = self.gen_space_secondary_index_type();
        let secondary_impl = self.gen_space_secondary_index_impl_space_index();
        let secondary_index_events = self.gen_space_secondary_index_events_type();

        quote! {
            #secondary_index_events
            #secondary_index
            #secondary_impl
        }
    }

    fn gen_space_secondary_index_events_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_events_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                quote! {
                    #i: Vec<indexset::cdc::change::ChangeEvent<
                        indexset::core::pair::Pair<#t, Link>
                    >>,
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }

    fn gen_space_secondary_index_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let ident = name_generator.get_space_secondary_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();

        let fields: Vec<_> = self
            .field_types
            .iter()
            .map(|(i, t)| {
                quote! {
                    #i: SpaceIndex<#t, { #inner_const_name as u32}>,
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            pub struct #ident {
                #(#fields)*
            }
        }
    }

    fn gen_space_secondary_index_impl_space_index(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let events_ident = name_generator.get_space_secondary_index_events_ident();
        let ident = name_generator.get_space_secondary_index_ident();

        let from_table_files_path_fn = self.gen_space_secondary_index_from_table_files_path_fn();
        let index_process_change_events_fn =
            self.gen_space_secondary_index_process_change_events_fn();

        quote! {
            impl SpaceSecondaryIndexOps<#events_ident> for #ident {
                #from_table_files_path_fn
                #index_process_change_events_fn
            }
        }
    }

    fn gen_space_secondary_index_from_table_files_path_fn(&self) -> TokenStream {
        let fields: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                let literal_name = Literal::string(i.to_string().as_str());
                quote! {
                    #i: SpaceIndex::secondary_from_table_files_path(path, #literal_name).await?,
                }
            })
            .collect();

        quote! {
            async fn from_table_files_path<S: AsRef<str>>(path: S) -> eyre::Result<Self> {
                let path = path.as_ref();
                Ok(Self {
                    #(#fields)*
                })
            }
        }
    }

    fn gen_space_secondary_index_process_change_events_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_index_ident(&self.struct_def.ident);
        let events_ident = name_generator.get_space_secondary_index_events_ident();

        let process: Vec<_> = self
            .field_types
            .keys()
            .map(|i| {
                quote! {
                    for event in events.#i {
                        self.#i.process_change_event(event).await?;
                    }
                }
            })
            .collect();

        quote! {
            async fn process_change_events(&mut self, events: #events_ident) -> eyre::Result<()> {
                #(#process)*
                core::result::Result::Ok(())
            }
        }
    }
}
