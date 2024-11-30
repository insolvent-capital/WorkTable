use std::fs::File;
use std::sync::Arc;

use worktable::prelude::*;

// TODO: Fix naming.
use crate::persistence::{get_empty_test_wt, get_test_wt, TestPersistWorkTable, TESTPERSIST_INNER_SIZE, TESTPERSIST_PAGE_SIZE};

#[test]
fn test_info_parse() {
    let mut file = File::open("tests/data/expected/test_persist.wt").unwrap();
    let info = parse_page::<SpaceInfoData, { TESTPERSIST_INNER_SIZE as u32 }>(&mut file, 0).unwrap();

    assert_eq!(info.header.space_id, 0.into());
    assert_eq!(info.header.page_id, 0.into());
    assert_eq!(info.header.previous_id, 0.into());
    assert_eq!(info.header.next_id, 1.into());
    assert_eq!(info.header.page_type, PageType::SpaceInfo);
    assert_eq!(info.header.data_length, 120);

    assert_eq!(info.inner.id, 0.into());
    assert_eq!(info.inner.page_count, 2);
    assert_eq!(info.inner.name, "Test");
    assert_eq!(info.inner.primary_key_intervals, vec![Interval(1, 1)]);
    assert!(info
        .inner
        .secondary_index_intervals
        .contains_key("another_idx"));
    assert_eq!(
        info.inner.secondary_index_intervals.get("another_idx"),
        Some(&vec![Interval(2, 2)])
    );
    assert_eq!(info.inner.data_intervals, vec![Interval(3, 3)]);
    assert_eq!(info.inner.empty_links_list, vec![]);
}

#[test]
fn test_index_parse() {
    let mut file = File::open("tests/data/expected/test_persist.wt").unwrap();
    let index = parse_page::<IndexData<u128>, { TESTPERSIST_PAGE_SIZE as u32 }>(&mut file, 1).unwrap();

    assert_eq!(index.header.space_id, 0.into());
    assert_eq!(index.header.page_id, 1.into());
    assert_eq!(index.header.previous_id, 0.into());
    assert_eq!(index.header.next_id, 2.into());
    assert_eq!(index.header.page_type, PageType::Index);
    assert_eq!(index.header.data_length, 3176);

    let mut key = 1;
    let length = 48;
    let mut offset = 0;
    let page_id = 0.into();

    for val in index.inner.index_values {
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
    let mut file = File::open("tests/data/expected/test_persist.wt").unwrap();
    let data = parse_data_page::<{ TESTPERSIST_PAGE_SIZE }, { TESTPERSIST_INNER_SIZE }>(&mut file, 3).unwrap();

    assert_eq!(data.header.space_id, 0.into());
    assert_eq!(data.header.page_id, 3.into());
    assert_eq!(data.header.previous_id, 2.into());
    assert_eq!(data.header.next_id, 0.into());
    assert_eq!(data.header.page_type, PageType::Data);
    assert_eq!(data.header.data_length, 4752);
}

#[test]
fn test_space_parse() {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "tests/data/expected".to_string(),
    });
    let table = TestPersistWorkTable::load_from_file(manager).unwrap();
    let expected = get_test_wt();

    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}

#[test]
fn test_space_parse_no_file() {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "tests/data/non-existent".to_string(),
    });
    let table = TestPersistWorkTable::load_from_file(manager).unwrap();
    let expected = get_empty_test_wt();
    assert_eq!(
        table.select_all().execute().unwrap(),
        expected.select_all().execute().unwrap()
    );
}
