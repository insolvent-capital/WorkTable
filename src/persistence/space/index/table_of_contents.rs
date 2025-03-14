use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use data_bucket::page::PageId;
use data_bucket::{
    parse_page, persist_page, GeneralHeader, GeneralPage, PageType, SizeMeasurable, SpaceId,
    TableOfContentsPage,
};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{rancor, Archive, Deserialize, Serialize};
use tokio::fs::File;

#[derive(Debug)]
pub struct IndexTableOfContents<T, const DATA_LENGTH: u32> {
    current_page: usize,
    next_page_id: Arc<AtomicU32>,
    pub pages: Vec<GeneralPage<TableOfContentsPage<T>>>,
}

impl<T, const DATA_LENGTH: u32> IndexTableOfContents<T, DATA_LENGTH>
where
    T: SizeMeasurable,
{
    pub fn new(space_id: SpaceId, next_page_id: Arc<AtomicU32>) -> Self {
        let page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
        let header = GeneralHeader::new(page_id.into(), PageType::IndexTableOfContents, space_id);
        let page = GeneralPage {
            header,
            inner: TableOfContentsPage::default(),
        };
        Self {
            current_page: 0,
            next_page_id,
            pages: vec![page],
        }
    }

    pub fn get(&self, node_id: &T) -> Option<PageId>
    where
        T: Ord + Eq,
    {
        for page in &self.pages {
            if page.inner.contains(node_id) {
                return Some(
                    page.inner
                        .get(node_id)
                        .expect("should exist as checked in `contains`"),
                );
            }
        }

        None
    }

    fn get_current_page_mut(&mut self) -> &mut GeneralPage<TableOfContentsPage<T>> {
        &mut self.pages[self.current_page]
    }

    pub fn insert(&mut self, node_id: T, page_id: PageId)
    where
        T: Clone + Ord + Eq + SizeMeasurable,
    {
        let next_page_id = self.next_page_id.clone();

        let page = self.get_current_page_mut();
        page.inner.insert(node_id.clone(), page_id);
        if page.inner.estimated_size() > DATA_LENGTH as usize {
            page.inner.remove_without_record(&node_id);
            if page.header.next_id.is_empty() {
                let next_page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
                let header = page.header.follow_with_page_id(next_page_id.into());
                page.header.next_id = next_page_id.into();
                self.pages.push(GeneralPage {
                    header,
                    inner: TableOfContentsPage::default(),
                });
                self.current_page += 1;

                let page = self.get_current_page_mut();
                page.inner.insert(node_id.clone(), page_id);
            } else {
                let mut i = self.current_page;
                while !self.pages[i].header.next_id.is_empty() {
                    i += 1;
                }
                self.current_page = i;
            }
        }
    }

    pub fn remove(&mut self, node_id: &T)
    where
        T: Clone + Ord + Eq + SizeMeasurable,
    {
        let mut removed = false;
        let mut i = 0;
        while !removed {
            let page = &mut self.pages[i];
            if page.inner.contains(node_id) {
                page.inner.remove(node_id);
                self.current_page = i;
                removed = true;
            }
            i += 1;
            if self.pages.len() == i {
                removed = true;
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, &PageId)> {
        self.pages.iter().flat_map(|v| v.inner.iter())
    }

    pub fn update_key(&mut self, old_key: &T, new_key: T)
    where
        T: Ord + Eq,
    {
        let page = self.get_current_page_mut();
        page.inner.update_key(old_key, new_key);
    }

    pub fn pop_empty_page_id(&mut self) -> Option<PageId> {
        let page = self.get_current_page_mut();
        page.inner.pop_empty_page()
    }

    pub async fn persist(&mut self, file: &mut File) -> eyre::Result<()>
    where
        T: Archive
            + Ord
            + Eq
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            > + Send
            + Sync,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
    {
        for page in &mut self.pages {
            persist_page(page, file).await?;
        }

        Ok(())
    }

    pub async fn parse_from_file(
        file: &mut File,
        space_id: SpaceId,
        next_page_id: Arc<AtomicU32>,
    ) -> eyre::Result<Self>
    where
        T: Archive
            + Ord
            + Eq
            + Clone
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rancor::Error>> + Ord + Eq,
    {
        let first_page = parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, 1).await;
        if let Ok(page) = first_page {
            if page.header.next_id.is_empty() {
                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    pages: vec![page],
                })
            } else {
                let mut table_of_contents_pages = vec![page];
                let mut index = 2;
                let mut ind = false;

                while !ind {
                    let page =
                        parse_page::<TableOfContentsPage<T>, DATA_LENGTH>(file, index).await?;
                    ind = page.header.next_id.is_empty();
                    table_of_contents_pages.push(page);
                    index += 1;
                }

                Ok(Self {
                    current_page: 0,
                    next_page_id,
                    pages: table_of_contents_pages,
                })
            }
        } else {
            Ok(Self::new(space_id, next_page_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::space::index::table_of_contents::IndexTableOfContents;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    #[test]
    fn empty() {
        let toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        assert_eq!(
            toc.current_page, 0,
            "`current_page` is not set to 0, it is {}",
            toc.current_page
        );
        assert_eq!(toc.pages.len(), 1, "`table_of_contents_pages` is empty")
    }

    #[test]
    fn insert_to_empty() {
        let mut toc = IndexTableOfContents::<u8, 128>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let key = 1;
        toc.insert(key, 1.into());

        let page = toc.pages[toc.current_page].clone();
        assert!(
            page.inner.contains(&key),
            "`page` not contains value {}, keys are {:?}",
            key,
            page.inner.into_iter().collect::<Vec<_>>()
        );
        assert!(
            page.inner.estimated_size() > 0,
            "`estimated_size` is zero, but it shouldn't"
        );
    }

    #[test]
    fn insert_more_than_one_page() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let mut keys = vec![];
        for key in 0..10 {
            toc.insert(key, 1.into());
            keys.push(key);
        }

        assert!(
            toc.current_page > 0,
            "`current_page` not moved forward and is {}",
            toc.current_page,
        );

        for i in 0..toc.current_page + 1 {
            let page = toc.pages[i].clone();
            for (k, _) in page.inner.into_iter() {
                let pos = keys.binary_search(&k).expect("value should exist");
                keys.remove(pos);
            }
        }

        assert!(keys.is_empty(), "Some keys was not inserted: {:?}", keys)
    }

    #[test]
    fn reinsert_on_empty_space() {
        let mut toc = IndexTableOfContents::<u8, 20>::new(0.into(), Arc::new(AtomicU32::new(0)));
        let mut keys = vec![];
        for key in 0..10 {
            toc.insert(key, 1.into());
            keys.push(key);
        }

        assert!(
            toc.current_page > 0,
            "`current_page` not moved forward and is {}",
            toc.current_page,
        );
        let before_remove_current_page = toc.current_page;

        let key_to_remove = keys[5];
        toc.remove(&key_to_remove);
        assert!(
            before_remove_current_page > toc.current_page,
            "`current_page` not moved backwards on remove and is still {}",
            toc.current_page,
        );
        assert_eq!(
            toc.get_current_page_mut().inner.clone().pop_empty_page(),
            Some(1.into()),
            "Current page not contains any empty page",
        );
        let after_remove_current_page = toc.current_page;

        let new_key = keys.last().unwrap() + 1;
        let id = toc.pop_empty_page_id().unwrap();
        toc.insert(new_key, id);
        assert_eq!(
            before_remove_current_page, toc.current_page,
            "`current_page` not moved back to before remove state and is {}",
            toc.current_page,
        );
        assert_eq!(
            toc.pages[after_remove_current_page]
                .inner
                .clone()
                .pop_empty_page(),
            None,
            "After insertion page contains empty page {:?}, but shouldn't",
            toc.pages[after_remove_current_page]
                .inner
                .clone()
                .pop_empty_page(),
        );
    }
}
