use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;
use proc_macro2::TokenStream;
use quote::quote;

impl Generator {
    pub fn get_persistence_task_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let ident = name_generator.get_persistence_task_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let space_secondary_indexes_events =
            name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();

        quote! {
            pub type #ident = PersistenceTask<
                <<#primary_key_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                #primary_key_type,
                #space_secondary_indexes_events,
                #avt_index_ident,
            >;
        }
    }

    pub fn get_persistence_engine_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let ident = name_generator.get_persistence_engine_ident();
        let primary_key_type = name_generator.get_primary_key_type_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let const_name = name_generator.get_page_size_const_ident();
        let space_secondary_indexes = name_generator.get_space_secondary_index_ident();
        let space_secondary_indexes_events =
            name_generator.get_space_secondary_index_events_ident();
        let avt_index_ident = name_generator.get_available_indexes_ident();
        let space_index_type = if self.attributes.pk_unsized {
            quote! {
                SpaceIndexUnsized<#primary_key_type, { #inner_const_name as u32 }>,
            }
        } else {
            quote! {
                SpaceIndex<#primary_key_type, { #inner_const_name as u32 }>,
            }
        };

        quote! {
            pub type #ident = PersistenceEngine<
                SpaceData<
                    <<#primary_key_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State,
                    { #inner_const_name},
                    { #const_name as u32 }
                >,
                #space_index_type
                #space_secondary_indexes,
                #primary_key_type,
                #space_secondary_indexes_events,
                #avt_index_ident,
            >;
        }
    }
}
