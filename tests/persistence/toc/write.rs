use data_bucket::INNER_PAGE_SIZE;
use std::fs::File;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use worktable::prelude::IndexTableOfContents;

use crate::{check_if_files_are_same, remove_file_if_exists};

#[test]
fn test_persist_index_table_of_contents() {
    remove_file_if_exists("tests/data/persist_index_table_of_contents.wt.idx".to_string());

    let mut toc = IndexTableOfContents::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        0.into(),
        Arc::new(AtomicU32::new(1)),
    );
    toc.insert(13, 1.into());
    let mut file = File::create("tests/data/persist_index_table_of_contents.wt.idx").unwrap();
    toc.persist(&mut file).unwrap();

    assert!(check_if_files_are_same(
        "tests/data/persist_index_table_of_contents.wt.idx".to_string(),
        "tests/data/expected/persist_index_table_of_contents.wt.idx".to_string()
    ))
}
