mod table_of_contents;
mod util;

use std::fmt::Debug;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use convert_case::{Case, Casing};
use data_bucket::page::{IndexValue, PageId};
use data_bucket::{
    get_index_page_size_from_data_length, parse_page, persist_page, GeneralHeader, GeneralPage,
    IndexPage, Link, PageType, SizeMeasurable, SpaceId, SpaceInfoPage, GENERAL_HEADER_SIZE,
};
use eyre::eyre;
use indexset::cdc::change::ChangeEvent;
use indexset::concurrent::map::BTreeMap;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};

use crate::persistence::space::open_or_create_file;
use crate::persistence::SpaceIndexOps;
use crate::prelude::WT_INDEX_EXTENSION;

pub use table_of_contents::IndexTableOfContents;
pub use util::map_index_pages_to_toc_and_general;

#[derive(Debug)]
pub struct SpaceIndex<T, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<T, DATA_LENGTH>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
    #[allow(dead_code)]
    info: GeneralPage<SpaceInfoPage<()>>,
}

impl<T, const DATA_LENGTH: u32> SpaceIndex<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
{
    pub fn new<S: AsRef<str>>(index_file_path: S, space_id: SpaceId) -> eyre::Result<Self> {
        let mut index_file = if !Path::new(index_file_path.as_ref()).exists() {
            let name = index_file_path
                .as_ref()
                .split("/")
                .collect::<Vec<_>>()
                .iter()
                .rev()
                .nth(1)
                .expect("is not in root...")
                .to_string()
                .from_case(Case::Snake)
                .to_case(Case::Pascal);
            let mut index_file = open_or_create_file(index_file_path.as_ref())?;
            Self::bootstrap(&mut index_file, name)?;
            index_file
        } else {
            open_or_create_file(index_file_path)?
        };
        let info = parse_page::<_, DATA_LENGTH>(&mut index_file, 0)?;

        let file_length = index_file.metadata()?.len();
        let page_id = file_length / (DATA_LENGTH as u64 + GENERAL_HEADER_SIZE as u64) + 1;
        let next_page_id = Arc::new(AtomicU32::new(page_id as u32));
        let table_of_contents =
            IndexTableOfContents::parse_from_file(&mut index_file, space_id, next_page_id.clone())?;
        Ok(Self {
            space_id,
            table_of_contents,
            next_page_id,
            index_file,
            info,
        })
    }

    fn add_new_index_page(&mut self, node_id: Pair<T, Link>, page_id: PageId) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let size = get_index_page_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut page = IndexPage::new(node_id.key.clone(), size);
        page.current_index = 1;
        page.current_length = 1;
        page.slots[0] = 0;
        page.index_values[0] = IndexValue {
            key: node_id.key,
            link: node_id.value,
        };
        self.add_index_page(page, page_id)
    }

    fn add_index_page(&mut self, node: IndexPage<T>, page_id: PageId) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
        let mut general_page = GeneralPage {
            inner: node,
            header,
        };
        persist_page(&mut general_page, &mut self.index_file)?;
        Ok(())
    }

    fn insert_on_index_page(
        &mut self,
        page_id: PageId,
        node_id: T,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<T>>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let mut new_node_id = None;

        let size = get_index_page_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut utility = IndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id)?;
        utility.slots.insert(index, utility.current_index);
        utility.slots.remove(size);
        utility.current_length += 1;
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        utility.current_index = IndexPage::<T>::persist_value(
            &mut self.index_file,
            page_id,
            size,
            index_value,
            utility.current_index,
        )?;

        if node_id < value.key {
            utility.node_id = value.key.clone();
            new_node_id = Some(value.key);
        }

        IndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility)?;

        Ok(new_node_id)
    }

    fn remove_from_index_page(
        &mut self,
        page_id: PageId,
        node_id: T,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<T>>
    where
        T: Archive
            + Default
            + Clone
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let mut new_node_id = None;

        let size = get_index_page_size_from_data_length::<T>(DATA_LENGTH as usize);
        let mut utility = IndexPage::<T>::parse_index_page_utility(&mut self.index_file, page_id)?;
        utility.current_index = *utility
            .slots
            .get(index)
            .expect("Slots should exist for every index within `size`");
        utility.slots.remove(index);
        utility.slots.push(0);
        utility.current_length -= 1;
        IndexPage::<T>::remove_value(&mut self.index_file, page_id, size, utility.current_index)?;

        if node_id == value.key {
            let index = *utility
                .slots
                .get(index - 1)
                .expect("slots always should exist in `size` bounds");
            utility.node_id = IndexPage::<T>::read_value_with_index(
                &mut self.index_file,
                page_id,
                size,
                index as usize,
            )?
            .key;
            new_node_id = Some(utility.node_id.clone())
        }

        IndexPage::<T>::persist_index_page_utility(&mut self.index_file, page_id, utility)?;

        Ok(new_node_id)
    }

    fn process_insert_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) =
            self.insert_on_index_page(page_id, node_id.clone(), index, value)?
        {
            self.table_of_contents.update_key(&node_id, new_node_id);
            self.table_of_contents.persist(&mut self.index_file)?;
        }
        Ok(())
    }
    fn process_remove_at(
        &mut self,
        node_id: T,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) =
            self.remove_from_index_page(page_id, node_id.clone(), index, value)?
        {
            self.table_of_contents.update_key(&node_id, new_node_id);
            self.table_of_contents.persist(&mut self.index_file)?;
        }
        Ok(())
    }
    fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents.insert(node_id.key.clone(), page_id);
        self.table_of_contents.persist(&mut self.index_file)?;
        self.add_new_index_page(node_id, page_id)?;

        Ok(())
    }

    fn process_remove_node(&mut self, node_id: T) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + SizeMeasurable
            + Ord
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
    {
        self.table_of_contents.remove(&node_id);
        self.table_of_contents.persist(&mut self.index_file)?;
        Ok(())
    }

    fn process_split_node(&mut self, node_id: T, split_index: usize) -> eyre::Result<()>
    where
        T: Archive
            + Clone
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let page_id = self
            .table_of_contents
            .get(&node_id)
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        let mut page =
            parse_page::<IndexPage<T>, DATA_LENGTH>(&mut self.index_file, page_id.into())?;
        let splitted_page = page.inner.split(split_index);
        let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };

        self.table_of_contents
            .update_key(&node_id, page.inner.node_id.clone());
        self.table_of_contents
            .insert(splitted_page.node_id.clone(), new_page_id);
        self.table_of_contents.persist(&mut self.index_file)?;

        self.add_index_page(splitted_page, new_page_id)?;
        persist_page(&mut page, &mut self.index_file)?;

        Ok(())
    }

    pub fn parse_indexset(&mut self) -> eyre::Result<BTreeMap<T, Link>>
    where
        T: Archive
            + Clone
            + Default
            + Debug
            + SizeMeasurable
            + Ord
            + Eq
            + Send
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>>,
    {
        let size = get_index_page_size_from_data_length::<T>(DATA_LENGTH as usize);
        let indexset = BTreeMap::with_maximum_node_size(size);
        for (_, page_id) in self.table_of_contents.iter() {
            let page =
                parse_page::<IndexPage<T>, DATA_LENGTH>(&mut self.index_file, (*page_id).into())?;
            let node = page.inner.get_node();
            indexset.attach_node(node)
        }

        Ok(indexset)
    }
}

impl<T, const DATA_LENGTH: u32> SpaceIndexOps<T> for SpaceIndex<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
{
    fn primary_from_table_files_path<S: AsRef<str>>(table_path: S) -> eyre::Result<Self> {
        let path = format!("{}/primary{}", table_path.as_ref(), WT_INDEX_EXTENSION);
        Self::new(path, 0.into())
    }

    fn secondary_from_table_files_path<S1: AsRef<str>, S2: AsRef<str>>(
        table_path: S1,
        name: S2,
    ) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        let path = format!(
            "{}/{}{}",
            table_path.as_ref(),
            name.as_ref(),
            WT_INDEX_EXTENSION
        );
        Self::new(path, 0.into())
    }

    fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()> {
        let info = SpaceInfoPage {
            id: 0.into(),
            page_count: 0,
            name: table_name,
            row_schema: vec![],
            primary_key_fields: vec![],
            secondary_index_types: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
        };
        let mut page = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 0.into()),
            inner: info,
        };
        persist_page(&mut page, file)
    }

    fn process_change_event(&mut self, event: ChangeEvent<Pair<T, Link>>) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                max_value: node_id,
                value,
                index,
            } => self.process_insert_at(node_id.key, value, index),
            ChangeEvent::RemoveAt {
                max_value: node_id,
                value,
                index,
            } => self.process_remove_at(node_id.key, value, index),
            ChangeEvent::CreateNode { max_value: node_id } => self.process_create_node(node_id),
            ChangeEvent::RemoveNode { max_value: node_id } => self.process_remove_node(node_id.key),
            ChangeEvent::SplitNode {
                max_value: node_id,
                split_index,
            } => self.process_split_node(node_id.key, split_index),
        }
    }
}

#[cfg(test)]
mod test {
    use data_bucket::{
        get_index_page_size_from_data_length, IndexPage, Persistable, INNER_PAGE_SIZE,
    };

    #[test]
    fn test_size_measure() {
        let size = get_index_page_size_from_data_length::<u32>(INNER_PAGE_SIZE);
        let page = IndexPage::new(0, size);
        assert!(page.as_bytes().as_ref().len() <= INNER_PAGE_SIZE)
    }
}
