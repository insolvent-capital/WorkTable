use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use data_bucket::page::PageId;
use data_bucket::{
    parse_page, persist_page, persist_pages_batch, GeneralHeader, GeneralPage, IndexPageUtility,
    IndexValue, Link, PageType, SizeMeasurable, SpaceId, SpaceInfoPage, UnsizedIndexPage,
    VariableSizeMeasurable,
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
use tokio::fs::File;

use crate::persistence::space::BatchChangeEvent;
use crate::persistence::{IndexTableOfContents, SpaceIndex, SpaceIndexOps};
use crate::prelude::WT_INDEX_EXTENSION;
use crate::UnsizedNode;

#[derive(Debug)]
pub struct SpaceIndexUnsized<T: Ord + Eq, const DATA_LENGTH: u32> {
    space_id: SpaceId,
    table_of_contents: IndexTableOfContents<(T, Link), DATA_LENGTH>,
    next_page_id: Arc<AtomicU32>,
    index_file: File,
    #[allow(dead_code)]
    info: GeneralPage<SpaceInfoPage<()>>,
}

impl<T, const DATA_LENGTH: u32> SpaceIndexUnsized<T, DATA_LENGTH>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq + Debug,
{
    pub async fn new<S: AsRef<str>>(index_file_path: S, space_id: SpaceId) -> eyre::Result<Self> {
        let space_index = SpaceIndex::<T, DATA_LENGTH>::new(index_file_path, space_id).await?;
        Ok(Self {
            space_id,
            table_of_contents: space_index.table_of_contents,
            next_page_id: space_index.next_page_id,
            index_file: space_index.index_file,
            info: space_index.info,
        })
    }

    async fn add_new_index_page(
        &mut self,
        node_id: Pair<T, Link>,
        page_id: PageId,
    ) -> eyre::Result<()> {
        let page = UnsizedIndexPage::new(node_id.clone().into())?;
        self.add_index_page(page, page_id).await
    }

    async fn add_index_page(
        &mut self,
        node: UnsizedIndexPage<T, DATA_LENGTH>,
        page_id: PageId,
    ) -> eyre::Result<()> {
        let header = GeneralHeader::new(page_id, PageType::Index, self.space_id);
        let mut general_page = GeneralPage {
            inner: node,
            header,
        };
        persist_page(&mut general_page, &mut self.index_file).await?;
        Ok(())
    }

    async fn process_create_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };
        self.table_of_contents
            .insert((node_id.key.clone(), node_id.value), page_id);
        self.table_of_contents.persist(&mut self.index_file).await?;
        self.add_new_index_page(node_id, page_id).await?;

        Ok(())
    }

    async fn process_remove_node(&mut self, node_id: Pair<T, Link>) -> eyre::Result<()> {
        self.table_of_contents.remove(&(node_id.key, node_id.value));
        self.table_of_contents.persist(&mut self.index_file).await?;
        Ok(())
    }

    async fn process_insert_at(
        &mut self,
        node_id: Pair<T, Link>,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) = self
            .insert_on_index_page(page_id, node_id.clone(), index, value)
            .await?
        {
            self.table_of_contents.update_key(
                &(node_id.key, node_id.value),
                (new_node_id.key, new_node_id.value),
            );
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
    }

    async fn insert_on_index_page(
        &mut self,
        page_id: PageId,
        node_id: Pair<T, Link>,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<Pair<T, Link>>> {
        let mut new_node_id = None;

        let mut utility = UnsizedIndexPage::<T, DATA_LENGTH>::parse_index_page_utility(
            &mut self.index_file,
            page_id,
        )
        .await?;
        let index_value = IndexValue {
            key: value.key.clone(),
            link: value.value,
        };
        let previous_offset = utility.last_value_offset;
        let value_offset = UnsizedIndexPage::<T, DATA_LENGTH>::persist_value(
            &mut self.index_file,
            page_id,
            previous_offset,
            index_value,
        )
        .await?;
        utility.slots_size += 1;
        utility.last_value_offset = value_offset;
        utility.slots.insert(
            index,
            (value_offset, (value_offset - previous_offset) as u16),
        );

        if node_id.key < value.key {
            utility.update_node_id(value.clone().into())?;
            new_node_id = Some(value);
        }

        UnsizedIndexPage::<T, DATA_LENGTH>::persist_index_page_utility(
            &mut self.index_file,
            page_id,
            utility,
        )
        .await?;

        Ok(new_node_id)
    }

    async fn process_remove_at(
        &mut self,
        node_id: Pair<T, Link>,
        value: Pair<T, Link>,
        index: usize,
    ) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        if let Some(new_node_id) = self
            .remove_from_index_page(page_id, node_id.clone(), index, value)
            .await?
        {
            self.table_of_contents.update_key(
                &(node_id.key, node_id.value),
                (new_node_id.key, new_node_id.value),
            );
            self.table_of_contents.persist(&mut self.index_file).await?;
        }
        Ok(())
    }

    async fn remove_from_index_page(
        &mut self,
        page_id: PageId,
        node_id: Pair<T, Link>,
        index: usize,
        value: Pair<T, Link>,
    ) -> eyre::Result<Option<Pair<T, Link>>> {
        let mut new_node_id = None;

        let mut utility = UnsizedIndexPage::<T, DATA_LENGTH>::parse_index_page_utility(
            &mut self.index_file,
            page_id,
        )
        .await?;
        utility.slots.remove(index);
        utility.slots_size -= 1;

        if node_id.key == value.key {
            let (offset, len) = *utility
                .slots
                .get(index - 1)
                .expect("slots always should exist in `size` bounds");
            let node_id = UnsizedIndexPage::<T, DATA_LENGTH>::read_value_with_offset(
                &mut self.index_file,
                page_id,
                offset,
                len,
            )
            .await?;
            utility.update_node_id(node_id)?;
            new_node_id = Some(utility.node_id.clone().into())
        }

        UnsizedIndexPage::<T, DATA_LENGTH>::persist_index_page_utility(
            &mut self.index_file,
            page_id,
            utility,
        )
        .await?;

        Ok(new_node_id)
    }

    async fn process_split_node(
        &mut self,
        node_id: Pair<T, Link>,
        split_index: usize,
    ) -> eyre::Result<()> {
        let page_id = self
            .table_of_contents
            .get(&(node_id.key.clone(), node_id.value))
            .ok_or(eyre!("Node with {:?} id is not found", node_id))?;
        let mut page = parse_page::<UnsizedIndexPage<T, DATA_LENGTH>, DATA_LENGTH>(
            &mut self.index_file,
            page_id.into(),
        )
        .await?;
        let splitted_page = page.inner.split(split_index);
        let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
            id
        } else {
            self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
        };

        self.table_of_contents.update_key(
            &(node_id.key, node_id.value),
            (page.inner.node_id.key.clone(), page.inner.node_id.link),
        );
        self.table_of_contents.insert(
            (
                splitted_page.node_id.key.clone(),
                splitted_page.node_id.link,
            ),
            new_page_id,
        );
        self.table_of_contents.persist(&mut self.index_file).await?;

        self.add_index_page(splitted_page, new_page_id).await?;
        persist_page(&mut page, &mut self.index_file).await?;

        Ok(())
    }

    pub async fn parse_indexset(
        &mut self,
    ) -> eyre::Result<BTreeMap<T, Link, UnsizedNode<Pair<T, Link>>>> {
        let indexset = BTreeMap::<T, Link, UnsizedNode<Pair<T, Link>>>::with_maximum_node_size(
            DATA_LENGTH as usize,
        );
        for (_, page_id) in self.table_of_contents.iter() {
            let page = parse_page::<UnsizedIndexPage<T, DATA_LENGTH>, DATA_LENGTH>(
                &mut self.index_file,
                (*page_id).into(),
            )
            .await?;
            let node = page.inner.get_node();
            indexset.attach_node(UnsizedNode::from_inner(node, DATA_LENGTH as usize))
        }

        Ok(indexset)
    }
}

impl<T, const INNER_PAGE_SIZE: u32> SpaceIndexOps<T> for SpaceIndexUnsized<T, INNER_PAGE_SIZE>
where
    T: Archive
        + Ord
        + Eq
        + Hash
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>>
        + Send
        + Sync
        + 'static,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq + Debug,
{
    async fn primary_from_table_files_path<S: AsRef<str> + Send>(
        table_path: S,
    ) -> eyre::Result<Self> {
        let path = format!("{}/primary{}", table_path.as_ref(), WT_INDEX_EXTENSION);
        Self::new(path, 0.into()).await
    }

    async fn secondary_from_table_files_path<S1: AsRef<str> + Send, S2: AsRef<str> + Send>(
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
        Self::new(path, 0.into()).await
    }

    async fn bootstrap(file: &mut File, table_name: String) -> eyre::Result<()> {
        SpaceIndex::<T, INNER_PAGE_SIZE>::bootstrap(file, table_name).await
    }

    async fn process_change_event(
        &mut self,
        event: ChangeEvent<Pair<T, Link>>,
    ) -> eyre::Result<()> {
        match event {
            ChangeEvent::InsertAt {
                event_id: _,
                max_value: node_id,
                value,
                index,
            } => self.process_insert_at(node_id, value, index).await,
            ChangeEvent::RemoveAt {
                event_id: _,
                max_value: node_id,
                value,
                index,
            } => self.process_remove_at(node_id, value, index).await,
            ChangeEvent::CreateNode {
                event_id: _,
                max_value: node_id,
            } => self.process_create_node(node_id).await,
            ChangeEvent::RemoveNode {
                event_id: _,
                max_value: node_id,
            } => self.process_remove_node(node_id).await,
            ChangeEvent::SplitNode {
                event_id: _,
                max_value: node_id,
                split_index,
            } => self.process_split_node(node_id, split_index).await,
        }
    }

    async fn process_change_event_batch(
        &mut self,
        events: BatchChangeEvent<T>,
    ) -> eyre::Result<()> {
        let mut pages: HashMap<PageId, _> = HashMap::new();
        for ev in events {
            match &ev {
                ChangeEvent::InsertAt { max_value, .. }
                | ChangeEvent::RemoveAt { max_value, .. } => {
                    let page_id = &(max_value.key.clone(), max_value.value);
                    let page_index = self
                        .table_of_contents
                        .get(page_id)
                        .expect("page should be available in table of contents");
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        // println!("Try to parse page: {:?} {:?}", page_index, page_id);
                        let page =
                            parse_page::<UnsizedIndexPage<T, INNER_PAGE_SIZE>, INNER_PAGE_SIZE>(
                                &mut self.index_file,
                                page_index.into(),
                            )
                            .await?;
                        // println!("Page {:?} {:?} parsed", page_index, page_id);
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    page_to_update.inner.apply_change_event(ev.clone())?;
                    if &(
                        page_to_update.inner.node_id.key.clone(),
                        page_to_update.inner.node_id.link,
                    ) != page_id
                    {
                        self.table_of_contents.update_key(
                            page_id,
                            (
                                page_to_update.inner.node_id.key.clone(),
                                page_to_update.inner.node_id.link,
                            ),
                        );
                    }
                }
                ChangeEvent::CreateNode {
                    event_id: _,
                    max_value,
                } => {
                    let page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };
                    self.table_of_contents
                        .insert((max_value.key.clone(), max_value.value), page_id);

                    let page =
                        UnsizedIndexPage::<T, INNER_PAGE_SIZE>::new(max_value.clone().into())?;
                    let header = GeneralHeader::new(page_id, PageType::IndexUnsized, self.space_id);
                    let general_page = GeneralPage {
                        inner: page,
                        header,
                    };
                    pages.insert(page_id, general_page);
                    self.table_of_contents
                        .insert((max_value.key.clone(), max_value.value), page_id)
                }
                ChangeEvent::RemoveNode {
                    event_id: _,
                    max_value,
                } => {
                    self.table_of_contents
                        .remove(&(max_value.key.clone(), max_value.value));
                }
                ChangeEvent::SplitNode {
                    event_id: _,
                    max_value,
                    split_index,
                } => {
                    let page_id = &(max_value.key.clone(), max_value.value);
                    let page_index = self
                        .table_of_contents
                        .get(page_id)
                        .expect("page should be available in table of contents");
                    let page = pages.get_mut(&page_index);
                    let page_to_update = if let Some(page) = page {
                        page
                    } else {
                        // println!("Try to parse page: {:?} {:?}", page_index, page_id);
                        let page =
                            parse_page::<UnsizedIndexPage<T, INNER_PAGE_SIZE>, INNER_PAGE_SIZE>(
                                &mut self.index_file,
                                page_index.into(),
                            )
                            .await?;
                        pages.insert(page_index, page);
                        pages
                            .get_mut(&page_index)
                            .expect("should be available as was just inserted before")
                    };
                    let splitted_page = page_to_update.inner.split(*split_index);

                    let new_page_id = if let Some(id) = self.table_of_contents.pop_empty_page_id() {
                        id
                    } else {
                        self.next_page_id.fetch_add(1, Ordering::Relaxed).into()
                    };

                    self.table_of_contents.update_key(
                        page_id,
                        (
                            page_to_update.inner.node_id.key.clone(),
                            page_to_update.inner.node_id.link,
                        ),
                    );
                    self.table_of_contents.insert(
                        (
                            splitted_page.node_id.key.clone(),
                            splitted_page.node_id.link,
                        ),
                        new_page_id,
                    );
                    let header = GeneralHeader::new(new_page_id, PageType::Index, self.space_id);
                    let general_page = GeneralPage {
                        inner: splitted_page,
                        header,
                    };
                    pages.insert(new_page_id, general_page);
                }
            }
        }

        self.table_of_contents.persist(&mut self.index_file).await?;
        persist_pages_batch(pages.values().cloned().collect(), &mut self.index_file).await?;
        Ok(())
    }
}
