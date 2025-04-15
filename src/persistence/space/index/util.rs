use crate::prelude::IndexTableOfContents;
use data_bucket::{
    GeneralHeader, GeneralPage, IndexPage, PageType, SizeMeasurable, UnsizedIndexPage,
    VariableSizeMeasurable,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

pub fn map_index_pages_to_toc_and_general<T, const DATA_LENGTH: u32>(
    pages: Vec<IndexPage<T>>,
) -> (
    IndexTableOfContents<T, DATA_LENGTH>,
    Vec<GeneralPage<IndexPage<T>>>,
)
where
    T: Clone + Default + Ord + Eq + SizeMeasurable,
{
    let mut general_index_pages = vec![];
    let next_page_id = Arc::new(AtomicU32::new(1));
    let mut toc = IndexTableOfContents::new(0.into(), next_page_id.clone());
    for page in pages {
        let page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
        toc.insert(page.node_id.clone(), page_id.into());
        let header = GeneralHeader::new(page_id.into(), PageType::Index, 0.into());
        let index_page = GeneralPage {
            inner: page,
            header,
        };
        general_index_pages.push(index_page)
    }

    (toc, general_index_pages)
}

pub fn map_unsized_index_pages_to_toc_and_general<T, const DATA_LENGTH: u32>(
    pages: Vec<UnsizedIndexPage<T, DATA_LENGTH>>,
) -> (
    IndexTableOfContents<T, DATA_LENGTH>,
    Vec<GeneralPage<UnsizedIndexPage<T, DATA_LENGTH>>>,
)
where
    T: Clone + Default + Ord + Eq + SizeMeasurable + VariableSizeMeasurable,
{
    let mut general_index_pages = vec![];
    let next_page_id = Arc::new(AtomicU32::new(1));
    let mut toc = IndexTableOfContents::new(0.into(), next_page_id.clone());
    for page in pages {
        let page_id = next_page_id.fetch_add(1, Ordering::Relaxed);
        toc.insert(page.node_id.clone(), page_id.into());
        let header = GeneralHeader::new(page_id.into(), PageType::IndexUnsized, 0.into());
        let index_page = GeneralPage {
            inner: page,
            header,
        };
        general_index_pages.push(index_page)
    }

    (toc, general_index_pages)
}
