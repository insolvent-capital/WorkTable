mod worktable_impls;

use proc_macro2::{Literal, TokenStream};
use quote::quote;

use crate::name_generator::WorktableNameGenerator;
use crate::persist_table::generator::Generator;

pub const WT_INFO_EXTENSION: &str = ".wt.info";
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
            pub struct #space_file_ident<const DATA_LENGTH: usize = #inner_const_name > {
                pub path: String,
                pub info: GeneralPage<SpaceInfoData>,
                pub primary_index: Vec<GeneralPage<IndexData<#pk_type>>>,
                pub indexes: #index_persisted_ident,
                pub data: Vec<GeneralPage<DataPage<DATA_LENGTH>>>,
            }
        }
    }

    fn gen_space_persist_impl(&self) -> TokenStream {
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let space_ident = name_generator.get_space_file_ident();
        let info_extension = Literal::string(WT_INFO_EXTENSION);
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            impl<const DATA_LENGTH: usize> #space_ident<DATA_LENGTH> {
                pub fn persist(&mut self) -> eyre::Result<()> {
                    let prefix = &self.path;
                    std::fs::create_dir_all(prefix)?;

                    {
                        let mut info_file = std::fs::File::create(format!("{}/{}", &self.path, #info_extension))?;
                        persist_page(&mut self.info, &mut info_file)?;
                    }
                    {
                        let mut primary_index_file = std::fs::File::create(format!("{}/primary{}", &self.path, #index_extension))?;
                        for mut primary_index_page in &mut self.primary_index {
                            persist_page(&mut primary_index_page, &mut primary_index_file)?;
                        }
                    }

                    self.indexes.persist(&prefix)?;

                    {
                        let mut data_file = std::fs::File::create(format!("{}/{}", &self.path, #data_extension))?;
                        for mut data_page in &mut self.data {
                            persist_page(&mut data_page, &mut data_file)?;
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

        quote! {
            impl #space_ident {
                #into_worktable_fn
                #parse_file_fn
            }
        }
    }

    fn gen_space_file_into_worktable_fn(&self) -> TokenStream {
        let wt_ident = &self.struct_def.ident;
        let name_generator = WorktableNameGenerator::from_struct_ident(&self.struct_def.ident);
        let index_ident = name_generator.get_index_type_ident();
        let index_type_ident = &self.index_type_ident;

        quote! {
            pub fn into_worktable(self, db_manager: std::sync::Arc<DatabaseManager>) -> #wt_ident {
                let mut page_id = 0;
                let data = self.data.into_iter().map(|p| {
                    let mut data = Data::from_data_page(p);
                    data.set_page_id(page_id.into());
                    page_id += 1;

                    std::sync::Arc::new(data)
                })
                    .collect();
                let data = DataPages::from_data(data)
                    .with_empty_links(self.info.inner.empty_links_list);
                let indexes = #index_ident::from_persisted(self.indexes);

                let pk_map = #index_type_ident::new();
                for page in self.primary_index {
                    for val in page.inner.index_values {
                        TableIndex::insert(&pk_map, val.key, val.link)
                            .expect("index is unique");
                    }
                }

                let table = WorkTable {
                    data,
                    pk_map,
                    indexes,
                    pk_gen: PrimaryKeyGeneratorState::from_state(self.info.inner.pk_gen_state),
                    lock_map: LockMap::new(),
                    table_name: "",
                    pk_phantom: std::marker::PhantomData
                };

                #wt_ident(
                    table,
                    db_manager
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
        let info_extension = Literal::string(WT_INFO_EXTENSION);
        let index_extension = Literal::string(WT_INDEX_EXTENSION);
        let data_extension = Literal::string(WT_DATA_EXTENSION);

        quote! {
            pub fn parse_file(path: &String) -> eyre::Result<Self> {
                let info = {
                    let mut info_file = std::fs::File::open(format!("{}/{}", path, #info_extension))?;
                    parse_page::<SpaceInfoData<<<#pk_type as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>, { #page_const_name as u32 }>(&mut info_file, 0)?
                };

                let mut primary_index = {
                    let mut primary_index = vec![];
                    let mut primary_file = std::fs::File::open(format!("{}/primary{}", path, #index_extension))?;
                    for interval in &info.inner.primary_key_intervals {
                        for page_id in interval.0..interval.1 {
                            let index = parse_page::<IndexData<#pk_type>, { #page_const_name as u32 }>(&mut primary_file, page_id as u32)?;
                            primary_index.push(index);
                        }
                        let index = parse_page::<IndexData<#pk_type>, { #page_const_name as u32 }>(&mut primary_file, interval.1 as u32)?;
                        primary_index.push(index);
                    }
                    primary_index
                };

                let indexes = #persisted_index_name::parse_from_file(path, &info.inner.secondary_index_intervals)?;
                let data = {
                    let mut data = vec![];
                    let mut data_file = std::fs::File::open(format!("{}/{}", path, #data_extension))?;
                    for interval in &info.inner.data_intervals {
                        for page_id in interval.0..interval.1 {
                            let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(&mut data_file, page_id as u32)?;
                            data.push(index);
                        }
                        let index = parse_data_page::<{ #page_const_name }, { #inner_const_name }>(&mut data_file, interval.1 as u32)?;
                        data.push(index);
                    }
                    data
                };

                Ok(Self {
                    path: "".to_string(),
                    info,
                    primary_index,
                    indexes,
                    data
                })
            }
        }
    }
}
