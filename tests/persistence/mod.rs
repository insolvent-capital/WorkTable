use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

mod read;
mod write;

worktable! (
    name: Test,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
);

pub fn get_empty_test_wt() -> TestWorkTable
{
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "test/data".to_string(),
    });

    TestWorkTable::new(manager)
}

pub fn get_test_wt() -> TestWorkTable {
    let table = get_empty_test_wt();

    for i in 1..100 {
        let row = TestRow {
            another: i as u64,
            id: i,
        };
        table.insert(row).unwrap();
    }

    table
}
