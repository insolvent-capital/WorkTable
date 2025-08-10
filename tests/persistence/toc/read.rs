use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use data_bucket::{INNER_PAGE_SIZE, Link};
use tokio::fs::OpenOptions;
use worktable::prelude::IndexTableOfContents;

#[tokio::test]
async fn test_index_table_of_contents_read() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/persist_index_table_of_contents.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let toc = IndexTableOfContents::<u32, { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(toc.get(&13), Some(1.into()))
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/test_persist/primary.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(1));
    let toc = IndexTableOfContents::<(u64, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();
    assert_eq!(
        toc.get(&(
            99,
            Link {
                page_id: 1.into(),
                offset: 2352,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_after_insert() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_updated_node_id() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            7,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48
            }
        )),
        Some(2.into())
    )
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_remove_at_node_id() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_remove_at_node_id.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            3,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48
            }
        )),
        Some(2.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_remove_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_remove_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            5,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        None
    );
    assert_eq!(
        toc.get(&(
            15,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(3.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_with_create_node_after_remove_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_create_node_after_remove.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            10,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(2.into())
    );
    assert_eq!(
        toc.get(&(
            15,
            Link {
                page_id: 1.into(),
                offset: 0,
                length: 24
            }
        )),
        Some(3.into())
    );
}

#[tokio::test]
async fn test_index_table_of_contents_read_from_space_index_after_split_node() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index/process_split_node.wt.idx")
        .await
        .unwrap();
    let next_id_gen = Arc::new(AtomicU32::new(2));
    let toc = IndexTableOfContents::<(u32, Link), { INNER_PAGE_SIZE as u32 }>::parse_from_file(
        &mut file,
        0.into(),
        next_id_gen,
    )
    .await
    .unwrap();

    assert_eq!(
        toc.get(&(
            1000,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 24
            }
        )),
        Some(3.into())
    );
    assert_eq!(
        toc.get(&(
            457,
            Link {
                page_id: 0.into(),
                offset: 10968,
                length: 24
            }
        )),
        Some(2.into())
    );
}
