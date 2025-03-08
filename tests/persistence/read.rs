use std::fs::File;

use worktable::prelude::*;

// TODO: Fix naming.
use crate::persistence::{
    get_empty_test_wt, get_test_wt, TestPersistRow, TestPersistWorkTable, TEST_PERSIST_INNER_SIZE,
    TEST_PERSIST_PAGE_SIZE, TEST_ROW_COUNT,
};
use crate::remove_dir_if_exists;

#[test]
fn test_info_parse() {
    let mut file = File::open("tests/data/expected/test_persist/.wt.data").unwrap();
    let info =
        parse_page::<SpaceInfoPage<u64>, { TEST_PERSIST_INNER_SIZE as u32 }>(&mut file, 0).unwrap();

    assert_eq!(info.header.space_id, 0.into());
    assert_eq!(info.header.page_id, 0.into());
    assert_eq!(info.header.previous_id, 0.into());
    assert_eq!(info.header.next_id, 0.into());
    assert_eq!(info.header.page_type, PageType::SpaceInfo);
    assert_eq!(info.header.data_length, 72);

    assert_eq!(info.inner.id, 0.into());
    assert_eq!(info.inner.page_count, 1);
    assert_eq!(info.inner.name, "TestPersist");
    assert_eq!(info.inner.pk_gen_state, 0);
    assert_eq!(info.inner.empty_links_list, vec![]);
}

#[test]
fn test_primary_index_parse() {
    let mut file = File::open("tests/data/expected/test_persist/primary.wt.idx").unwrap();
    let index =
        parse_page::<IndexPage<u64>, { TEST_PERSIST_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();

    assert_eq!(index.header.space_id, 0.into());
    assert_eq!(index.header.page_id, 2.into());
    assert_eq!(index.header.previous_id, 0.into());
    assert_eq!(index.header.next_id, 0.into());
    assert_eq!(index.header.page_type, PageType::Index);
    assert_eq!(index.header.data_length, 16334);

    let mut key = 1;
    let length = 24;
    let mut offset = 0;
    let page_id = 1.into();

    for val in &index.inner.index_values[..index.inner.current_length as usize] {
        assert_eq!(val.key, key);
        assert_eq!(
            val.link,
            Link {
                page_id,
                offset,
                length,
            }
        );

        key += 1;
        offset += length;
    }
}

#[test]
fn test_another_idx_index_parse() {
    let mut file = File::open("tests/data/expected/test_persist/another_idx.wt.idx").unwrap();
    let index =
        parse_page::<IndexPage<u64>, { TEST_PERSIST_PAGE_SIZE as u32 }>(&mut file, 2).unwrap();

    assert_eq!(index.header.space_id, 0.into());
    assert_eq!(index.header.page_id, 2.into());
    assert_eq!(index.header.previous_id, 0.into());
    assert_eq!(index.header.next_id, 0.into());
    assert_eq!(index.header.page_type, PageType::Index);
    assert_eq!(index.header.data_length, 16386);

    let mut key = 1;
    let length = 24;
    let mut offset = 0;
    let page_id = 1.into();

    for val in &index.inner.index_values[..index.inner.current_length as usize] {
        assert_eq!(val.key, key);
        assert_eq!(
            val.link,
            Link {
                page_id,
                offset,
                length,
            }
        );

        key += 1;
        offset += length;
    }
}

#[test]
fn test_data_parse() {
    let mut file = File::open("tests/data/expected/test_persist/.wt.data").unwrap();
    let data =
        parse_data_page::<{ TEST_PERSIST_PAGE_SIZE }, { TEST_PERSIST_INNER_SIZE }>(&mut file, 1)
            .unwrap();

    assert_eq!(data.header.space_id, 0.into());
    assert_eq!(data.header.page_id, 1.into());
    assert_eq!(data.header.previous_id, 0.into());
    assert_eq!(data.header.next_id, 0.into());
    assert_eq!(data.header.page_type, PageType::Data);
    assert_eq!(data.header.data_length, 2376);
}

#[tokio::test]
async fn test_space_parse() {
    let config = PersistenceConfig::new("tests/data/expected", "tests/data/expected").unwrap();
    let table = TestPersistWorkTable::load_from_file(config).unwrap();
    let expected = get_test_wt();

    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}

#[tokio::test]
async fn test_space_parse_no_file() {
    remove_dir_if_exists("tests/non-existent".to_string());

    let config = PersistenceConfig::new("tests/non-existent", "tests/non-existent").unwrap();
    let table = TestPersistWorkTable::load_from_file(config).unwrap();
    let expected = get_empty_test_wt();
    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}

#[tokio::test]
async fn test_space_insert_after_read() {
    let config = PersistenceConfig::new("tests/data/expected", "tests/data/expected").unwrap();
    let table = TestPersistWorkTable::load_from_file(config).unwrap();

    let row = TestPersistRow {
        another: TEST_ROW_COUNT as u64,
        id: TEST_ROW_COUNT as u64,
    };
    table.insert(row.clone()).unwrap();
    let expected = get_test_wt();
    expected.insert(row).unwrap();

    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}

#[tokio::test]
async fn test_space_delete_after_read() {
    let config = PersistenceConfig::new("tests/data/expected", "tests/data/expected").unwrap();
    let table = TestPersistWorkTable::load_from_file(config).unwrap();

    table
        .delete((TEST_ROW_COUNT as u64 - 1).into())
        .await
        .unwrap();
    let expected = get_test_wt();
    expected
        .delete((TEST_ROW_COUNT as u64 - 1).into())
        .await
        .unwrap();

    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}
