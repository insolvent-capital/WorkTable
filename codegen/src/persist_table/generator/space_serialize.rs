use convert_case::{Case, Casing};
use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_type(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let pk_type = &self.pk_ident;
        let name_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());
        let index_persisted_ident = Ident::new(
            format!("{}IndexPersisted", name).as_str(),
            Span::mixed_site(),
        );
        let inner_const_name = Ident::new(
            format!("{}_INNER_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );

        Ok(quote! {
            #[derive(Debug)]
            pub struct #name_ident<const DATA_LENGTH: usize = #inner_const_name > {
                pub path: String,

                pub info: GeneralPage<SpaceInfoData>,
                pub primary_index: Vec<GeneralPage<IndexData<#pk_type>>>,
                pub indexes: #index_persisted_ident,
                pub data: Vec<GeneralPage<DataPage<DATA_LENGTH>>>,
            }
        })
    }

    pub fn gen_space_impls(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let space_info_fn = self.gen_space_info_fn()?;
        let persisted_pk_fn = self.gen_persisted_primary_key_fn()?;
        let into_space = self.gen_into_space()?;

        let persist_fn = self.gen_persist_fn()?;
        let from_file_fn = self.gen_from_file_fn()?;

        let space_persist = self.gen_space_persist_fn()?;

        Ok(quote! {
            impl #ident {
                #space_info_fn
                #persisted_pk_fn
                #into_space

                #persist_fn
                #from_file_fn
            }

            #space_persist
        })
    }

    fn gen_persist_fn(&self) -> syn::Result<TokenStream> {
        Ok(quote! {
            pub fn persist(&self) -> eyre::Result<()> {
                let mut space = self.into_space();
                space.persist()?;
                Ok(())
            }
        })
    }

    fn gen_from_file_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());
        let wt_ident = self.struct_def.ident.clone();
        let name_underscore = name.from_case(Case::Pascal).to_case(Case::Snake);

        Ok(quote! {
            pub fn load_from_file(manager: std::sync::Arc<DatabaseManager>) -> eyre::Result<Self> {
                let filename = format!("{}/{}.wt", manager.database_files_dir.as_str(), #name_underscore);
                let filename = std::path::Path::new(filename.as_str());
                let Ok(mut file) = std::fs::File::open(filename) else {
                    return Ok(#wt_ident::new(manager));
                };
                let space = #space_ident::parse_file(&mut file)?;
                let table = space.into_worktable(manager);
                Ok(table)
            }
        })
    }

    fn gen_space_info_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let pk = &self.pk_ident;
        let literal_name = Literal::string(name.as_str());

        Ok(quote! {
            pub fn space_info_default() -> GeneralPage<SpaceInfoData<<<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State>> {
                let inner = SpaceInfoData {
                    id: 0.into(),
                    page_count: 0,
                    name: #literal_name.to_string(),
                    primary_key_intervals: vec![],
                    secondary_index_intervals: std::collections::HashMap::new(),
                    data_intervals: vec![],
                    pk_gen_state: <<#pk as TablePrimaryKey>::Generator as PrimaryKeyGeneratorState>::State::default(),
                    empty_links_list: vec![]
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
        })
    }

    fn gen_persisted_primary_key_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let const_name = Ident::new(
            format!("{}_INNER_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let pk_type = &self.pk_ident;

        Ok(quote! {
            pub fn get_peristed_primary_key(&self) -> Vec<IndexData<#pk_type>> {
                map_unique_tree_index::<_, #const_name>(&self.0.pk_map)
            }
        })
    }

    fn gen_into_space(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let const_name = Ident::new(
            format!("{}_INNER_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());

        Ok(quote! {
            pub fn into_space(&self) -> #space_ident<#const_name> {
                let path = self.1.config_path.clone();

                let mut info = #ident::space_info_default();
                info.inner.pk_gen_state = self.0.pk_gen.get_state();
                info.inner.empty_links_list = self.0.data.get_empty_links();
                info.inner.page_count = 1;
                let mut header = &mut info.header;

                let mut primary_index = map_index_pages_to_general(
                    self.get_peristed_primary_key(),
                    &mut header
                );
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
                let mut indexes = self.0.indexes.get_persisted_index(previous_header);
                let secondary_intevals = indexes.get_intervals();
                info.inner.secondary_index_intervals = secondary_intevals;

                let previous_header = match indexes.get_last_header_mut() {
                    Some(previous_header) => previous_header,
                    None => previous_header,
                };
                let data = map_data_pages_to_general(self.0.data.get_bytes().into_iter().map(|(b, offset)| DataPage {
                    data: b,
                    length: offset,
                }).collect::<Vec<_>>(), previous_header);
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
        })
    }

    fn gen_space_persist_fn(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());
        let file_name = Literal::string(format!("{}.wt", name.from_case(Case::Pascal).to_case(Case::Snake)).as_str());

        Ok(quote! {
            impl<const DATA_LENGTH: usize> #space_ident<DATA_LENGTH> {
                pub fn persist(&mut self) -> eyre::Result<()> {
                    let file_name = #file_name;
                    let path = std::path::Path::new(format!("{}/{}", &self.path , file_name).as_str());
                    let prefix = &self.path;
                    std::fs::create_dir_all(prefix).unwrap();

                    let mut file = std::fs::File::create(format!("{}/{}", &self.path , file_name))?;
                    persist_page(&mut self.info, &mut file)?;

                    for mut primary_index_page in &mut self.primary_index {
                        persist_page(&mut primary_index_page, &mut file)?;
                    }
                    self.indexes.persist(&mut file)?;
                    for mut data_page in &mut self.data {
                        persist_page(&mut data_page, &mut file)?;
                    }

                    Ok(())
                }
            }
        })
    }
}
