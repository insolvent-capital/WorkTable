mod worktable_impls;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

pub const WT_INDEX_EXTENSION: &str = ".wt.idx";
pub const WT_DATA_EXTENSION: &str = ".wt.data";

impl Generator {
    pub fn gen_space_file_def(&self) -> TokenStream {
        let type_ = self.gen_space_file_type();
        let impls = self.gen_space_file_impls();
        let worktable_impl = self.gen_space_file_worktable_impl();
        let space_persist_impl = self.gen_space_persist_impl();

        quote! {
            #type_
            #impls
            #worktable_impl
            #space_persist_impl
        }
    }

    fn gen_space_file_type(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_persisted_ident = name_generator.get_persisted_index_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let pk_type = name_generator.get_primary_key_type_ident();
        let space_file_ident = name_generator.get_space_file_ident();

        quote! {
            #[derive(Debug)]
            pub struct #space_file_ident {
                pub path: String,
                pub primary_index: (Vec<GeneralPage<TableOfContentsPage<#pk_type>>>, Vec<GeneralPage<IndexPage<#pk_type>>>),
                pub indexes: #index_persisted_ident,
                pub data: Vec<GeneralPage<DataPage<#inner_const_name>>>,
                pub data_info: GeneralPage<SpaceInfoPage<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>>,
            }
        }
    }

    fn gen_space_file_get_primary_index_info_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let literal_name = name_generator.get_work_table_literal_name();

        quote! {
            fn get_primary_index_info(&self) -> eyre::Result<GeneralPage<SpaceInfoPage<()>>> {
                let mut info = {
                    let inner = SpaceInfoPage {
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    pk_gen_state: (),
                    empty_links_list: vec![],
                    primary_key_fields: vec![],
                    row_schema: vec![],
                    secondary_index_types: vec![],
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
                };
                info.inner.page_count = self.primary_index.0.len() as u32 + self.primary_index.1.len() as u32;
                Ok(info)
            }
        }
    }

    fn gen_space_persist_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            impl #space_ident {
                pub async fn persist(&mut self) -> eyre::Result<()> {
                    let prefix = &self.path;
                    tokio::fs::create_dir_all(prefix).await?;

                    {
                        let mut primary_index_file = tokio::fs::File::create(format!("{}/primary{}", &self.path, #index_extension)).await?;
                        let mut info = self.get_primary_index_info()?;
                        persist_page(&mut info, &mut primary_index_file).await?;
                        for mut toc_page in &mut self.primary_index.0 {
                            persist_page(&mut toc_page, &mut primary_index_file).await?;
                        }
                        for mut primary_index_page in &mut self.primary_index.1 {
                            persist_page(&mut primary_index_page, &mut primary_index_file).await?;
                        }
                    }

                    self.indexes.persist(&prefix).await?;

                    {
                        let mut data_file = tokio::fs::File::create(format!("{}/{}", &self.path, #data_extension)).await?;
                        persist_page(&mut self.data_info, &mut data_file).await?;
                        for mut data_page in &mut self.data {
                            persist_page(&mut data_page, &mut data_file).await?;
                        }
                    }

                    Ok(())
                }
            }
        }
    }

    pub fn gen_space_file_impls(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();

        let into_worktable_fn = self.gen_space_file_into_worktable_fn();
        let parse_file_fn = self.gen_space_file_parse_file_fn();
        let get_primary_index_info_fn = self.gen_space_file_get_primary_index_info_fn();

        quote! {
            impl #space_ident {
                #into_worktable_fn
                #parse_file_fn
                #get_primary_index_info_fn
            }
        }
    }

    fn gen_space_file_into_worktable_fn(&self) -> TokenStream {
        let wt_ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_ident = name_generator.get_index_type_ident();
        let task_ident = name_generator.get_persistence_task_ident();
        let engine_ident = name_generator.get_persistence_engine_ident();
        let dir_name = name_generator.get_dir_name();

        quote! {
            pub async fn into_worktable(self, config: PersistenceConfig) -> #wt_ident {
                let mut page_id = 1;
                let data = self.data.into_iter().map(|p| {
                    let mut data = Data::from_data_page(p);
                    data.set_page_id(page_id.into());
                    page_id += 1;

                    std::sync::Arc::new(data)
                })
                    .collect();
                let data = DataPages::from_data(data)
                    .with_empty_links(self.data_info.inner.empty_links_list);
                let indexes = #index_ident::from_persisted(self.indexes);

                let pk_map = IndexMap::new();
                for page in self.primary_index.1 {
                    let node = page.inner.get_node();
                    pk_map.attach_node(node);
                }

                let table = WorkTable {
                    data,
                    pk_map,
                    indexes,
                    pk_gen: PrimaryKeyGeneratorState::from_state(self.data_info.inner.pk_gen_state),
                    lock_map: LockMap::new(),
                    table_name: "",
                    pk_phantom: std::marker::PhantomData,
                    types_phantom: std::marker::PhantomData,
                };

                let path = format!("{}/{}", config.tables_path.as_str(), #dir_name);
                let engine: #engine_ident = PersistenceEngine::from_table_files_path(path)
                                .await
                                .expect("should not panic as SpaceFile is ok");
                #wt_ident(
                    table,
                    config,
                    #task_ident::run_engine(engine)
                )
            }
        }
    }

    fn gen_space_file_parse_file_fn(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let pk_type = name_generator.get_primary_key_type_ident();
        let page_const_name = name_generator.get_page_size_const_ident();
        let inner_const_name = name_generator.get_page_inner_size_const_ident();
        let persisted_index_name = name_generator.get_persisted_index_ident();
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            pub async fn parse_file(path: &str) -> eyre::Result<Self> {
                let mut primary_index = {
                    let mut primary_index = vec![];
                    let mut primary_file = tokio::fs::File::open(format!("{}/primary{}", path, #index_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<()>, { #page_const_name as u32 }>(&mut primary_file, 0).await?;
                    let file_length = primary_file.metadata().await?.len();
                    let count = file_length / (#page_const_name as u64 + GENERAL_HEADER_SIZE as u64);
                    let next_page_id = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(count as u32));
                    let toc = IndexTableOfContents::<_, { #page_const_name as u32 }>::parse_from_file(&mut primary_file, 0.into(), next_page_id.clone()).await?;
                    for page_id in toc.iter().map(|(_, page_id)| page_id) {
                        let index = parse_page::<IndexPage<#pk_type>, { #page_const_name as u32 }>(&mut primary_file, (*page_id).into()).await?;
                        primary_index.push(index);
                    }
                    (toc.pages, primary_index)
                };

                let indexes = #persisted_index_name::parse_from_file(path).await?;
                let (data, data_info) = {
                    let mut data = vec![];
                    let mut data_file = tokio::fs::File::open(format!("{}/{}", path, #data_extension)).await?;
                    let info = parse_page::<SpaceInfoPage<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(&mut data_file, 0).await?;
                    let file_length = data_file.metadata().await?.len();
                    let count = file_length / (#inner_const_name as u64 + GENERAL_HEADER_SIZE as u64);
                    for page_id in 1..=count {
                        let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(&mut data_file, page_id as u32).await?;
                        data.push(index);
                    }
                    (data, info)
                };

                Ok(Self {
                    path: "".to_string(),
                    primary_index,
                    indexes,
                    data,
                    data_info
                })
            }
        }
    }
}
