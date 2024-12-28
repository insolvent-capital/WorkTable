use proc_macro2::TokenStream;
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_file_worktable_impl(&self) -> TokenStream {
        let ident = &self.struct_def.ident;
        let space_info_fn = self.gen_worktable_space_info_fn();
        let persisted_pk_fn = self.gen_worktable_persisted_primary_key_fn();
        let into_space = self.gen_worktable_into_space();
        let persist_fn = self.gen_worktable_persist_fn();
        let from_file_fn = self.gen_worktable_from_file_fn();

        quote! {
            impl #ident {
                #space_info_fn
                #persisted_pk_fn
                #into_space
                #persist_fn
                #from_file_fn
            }
        }
    }

    fn gen_worktable_persist_fn(&self) -> TokenStream {
        quote! {
            pub fn persist(&self) -> eyre::Result<()> {
                let mut space = self.into_space();
                space.persist()?;
                Ok(())
            }
        }
    }

    fn gen_worktable_from_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();
        let wt_ident = name_generator.get_work_table_ident();
        let dir_name = name_generator.get_dir_name();

        quote! {
            pub fn load_from_file(manager: std::sync::Arc<DatabaseManager>) -> eyre::Result<Self> {
                let filename = format!("{}/{}", manager.database_files_dir.as_str(), #dir_name);
                if !std::path::Path::new(filename.as_str()).exists() {
                    return Ok(#wt_ident::new(manager));
                };
                let space = #space_ident::parse_file(&filename)?;
                let table = space.into_worktable(manager);
                Ok(table)
            }
        }
    }

    fn gen_worktable_space_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk = name_generator.get_primary_key_type_ident();
        let literal_name = name_generator.get_work_table_literal_name();

        quote! {
            pub fn space_info_default() -> GeneralPage<SpaceInfoData<<<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>> {
                let inner = SpaceInfoData {
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    primary_key_intervals: vec![],
                    secondary_index_intervals: std::collections::HashMap::new(),
                    data_intervals: vec![],
                    pk_gen_state: <<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State::default(),
                    empty_links_list: vec![],
                    secondary_index_map: std::collections::HashMap::default()
                };
                let header = GeneralHeader {
                    data_version: DATA_VERSION,
                    page_id: 0.into(),
                    previous_id: 0.into(),
                    next_id: 0.into(),
                    page_type: PageType::SpaceInfo,
                    space_id: 0.into(),
                    data_length: 0,
                };
                GeneralPage {
                    header,
                    inner
                }
            }
        }
    }

    fn gen_worktable_persisted_primary_key_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();

        quote! {
            pub fn get_peristed_primary_key(&self) -> Vec<IndexData<#pk_type>> {
                map_unique_tree_index::<_, #const_name>(TableIndex::iter(&self.0.pk_map))
            }
        }
    }

    fn gen_worktable_into_space(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let ident = name_generator.get_work_table_ident();
        let const_name = name_generator.get_page_inner_size_const_ident();
        let space_ident = name_generator.get_space_file_ident();
        let dir_name = name_generator.get_dir_name();

        quote! {
            pub fn into_space(&self) -> #space_ident<#const_name> {
                let path = format!("{}/{}", self.1.config_path.clone(), #dir_name);

                let mut info = #ident::space_info_default();
                info.inner.pk_gen_state = self.0.pk_gen.get_state();
                info.inner.empty_links_list = self.0.data.get_empty_links();
                info.inner.page_count = 1;
                let mut header = &mut info.header;

                let mut primary_index = map_index_pages_to_general(self.get_peristed_primary_key());
                let interval = Interval(
                    primary_index.first()
                        .expect("Primary index page always exists, even if empty")
                        .header
                        .page_id
                        .into(),
                    primary_index.last()
                        .expect("Primary index page always exists, even if empty")
                        .header
                        .page_id
                        .into()
                );
                info.inner.page_count += primary_index.len() as u32;

                info.inner.primary_key_intervals = vec![interval];
                let previous_header = &mut primary_index
                    .last_mut()
                    .expect("Primary index page always exists, even if empty")
                    .header;
                let mut indexes = self.0.indexes.get_persisted_index();
                let secondary_intevals = indexes.get_intervals();
                info.inner.secondary_index_intervals = secondary_intevals;

                let previous_header = match indexes.get_last_header_mut() {
                    Some(previous_header) => previous_header,
                    None => previous_header,
                };
                let data = map_data_pages_to_general(self.0.data.get_bytes().into_iter().map(|(b, offset)| DataPage {
                    data: b,
                    length: offset,
                }).collect::<Vec<_>>());
                let interval = Interval(
                    data
                        .first()
                        .expect("Data page always exists, even if empty")
                        .header
                        .page_id
                        .into(),
                    data
                        .last()
                        .expect("Data page always exists, even if empty")
                        .header
                        .page_id
                        .into()
                );
                info.inner.data_intervals = vec![interval];

                #space_ident {
                    path,
                    info,
                    primary_index,
                    indexes,
                    data,
                }
            }
        }
    }
}
