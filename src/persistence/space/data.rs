use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;

use crate::persistence::space::{open_or_create_file, BatchData};
use crate::persistence::SpaceDataOps;
use crate::prelude::WT_DATA_EXTENSION;
use convert_case::{Case, Casing};
use data_bucket::{
    parse_data_pages_batch, parse_general_header_by_index, parse_page, persist_page,
    persist_pages_batch, update_at, DataPage, GeneralHeader, GeneralPage, Link, PageType,
    Persistable, SizeMeasurable, SpaceInfoPage,
};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub struct SpaceData<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> {
    pub info: GeneralPage<SpaceInfoPage<PkGenState>>,
    pub last_page_id: u32,
    pub current_data_length: u32,
    pub data_file: File,
}

impl<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32>
    SpaceData<PkGenState, INNER_PAGE_SIZE, PAGE_SIZE>
{
    async fn update_data_length(&mut self) -> eyre::Result<()> {
        let offset = (u32::default().aligned_size() * 6) as u32;
        self.data_file
            .seek(SeekFrom::Start(
                (self.last_page_id * PAGE_SIZE + offset) as u64,
            ))
            .await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.current_data_length)?;
        self.data_file.write_all(bytes.as_ref()).await?;
        Ok(())
    }
}

impl<PkGenState, const INNER_PAGE_SIZE: usize, const PAGE_SIZE: u32> SpaceDataOps<PkGenState>
    for SpaceData<PkGenState, INNER_PAGE_SIZE, PAGE_SIZE>
where
    PkGenState: Default
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Archive
        + Send
        + Sync,
    <PkGenState as Archive>::Archived:
        Deserialize<PkGenState, HighDeserializer<rkyv::rancor::Error>>,
    SpaceInfoPage<PkGenState>: Persistable,
{
    async fn from_table_files_path<S: AsRef<str> + Send>(table_path: S) -> eyre::Result<Self> {
        let path = format!("{}/{}", table_path.as_ref(), WT_DATA_EXTENSION);
        let mut data_file = if !Path::new(&path).exists() {
            let name = table_path
                .as_ref()
                .split("/")
                .last()
                .expect("is not in root...")
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let mut data_file = open_or_create_file(path).await?;
            Self::bootstrap(&mut data_file, name).await?;
            data_file
        } else {
            open_or_create_file(path).await?
        };
        let info = parse_page::<_, PAGE_SIZE>(&mut data_file, 0).await?;
        let file_length = data_file.metadata().await?.len();
        let page_id = file_length / PAGE_SIZE as u64;
        let last_page_header =
            parse_general_header_by_index(&mut data_file, page_id as u32).await?;

        Ok(Self {
            data_file,
            info,
            last_page_id: page_id as u32,
            current_data_length: last_page_header.data_length,
        })
    }

    async fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()> {
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: table_name,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            pk_gen_state: Default::default(),
            empty_links_list: vec![],
        };
        let mut page = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 0.into()),
            inner: info,
        };
        persist_page(&mut page, file).await
    }

    async fn save_data(&mut self, link: Link, bytes: &[u8]) -> eyre::Result<()> {
        if link.page_id > self.last_page_id.into() {
            let mut page = GeneralPage {
                header: GeneralHeader::new(link.page_id, PageType::Data, 0.into()),
                inner: DataPage {
                    length: 0,
                    data: [0; 1],
                },
            };
            persist_page(&mut page, &mut self.data_file).await?;
            self.current_data_length = 0;
            self.last_page_id += 1;
        }
        self.current_data_length += link.length;
        self.update_data_length().await?;
        update_at::<{ PAGE_SIZE }>(&mut self.data_file, link, bytes).await
    }

    async fn save_batch_data(&mut self, batch_data: BatchData) -> eyre::Result<()> {
        let page_ids = batch_data.keys().map(|id| (*id).into()).collect::<Vec<_>>();
        let ids_to_create = page_ids
            .iter()
            .filter(|id| **id > self.last_page_id)
            .cloned()
            .collect::<Vec<_>>();
        let ids_to_parse = page_ids
            .iter()
            .filter(|id| **id <= self.last_page_id)
            .cloned()
            .collect::<Vec<_>>();

        if let Some(max) = ids_to_create.last() {
            self.last_page_id = *max;
        }
        let created_pages = ids_to_create
            .into_iter()
            .map(|id| GeneralPage {
                header: GeneralHeader::new(id.into(), PageType::Data, 0.into()),
                inner: DataPage {
                    length: 0,
                    data: [0; INNER_PAGE_SIZE],
                },
            })
            .collect::<Vec<_>>();
        let parsed_pages =
            parse_data_pages_batch::<PAGE_SIZE, INNER_PAGE_SIZE>(&mut self.data_file, ids_to_parse)
                .await?;

        let updated_pages = vec![parsed_pages, created_pages]
            .into_iter()
            .flatten()
            .map(|mut page| {
                let id = page.header.page_id;
                let ops = batch_data
                    .get(&id)
                    .expect("should be available as pages parsed from these ids");
                for (link, bytes) in ops {
                    page.inner.update_at(*link, bytes)?;
                }
                Ok::<_, eyre::Report>(page)
            })
            .collect::<Result<Vec<_>, _>>()?;

        persist_pages_batch(updated_pages, &mut self.data_file).await?;

        Ok(())
    }

    fn get_mut_info(&mut self) -> &mut GeneralPage<SpaceInfoPage<PkGenState>> {
        &mut self.info
    }

    fn save_info(&mut self) -> impl Future<Output = eyre::Result<()>> + Send {
        persist_page(&mut self.info, &mut self.data_file)
    }
}
