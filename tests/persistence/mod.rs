use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

mod read;
mod write;

worktable! (
    name: TestPersist,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
);

worktable! (
    name: TestWithoutSecondaryIndexes,
    persist: true,
    columns: {
        id: u128 primary_key,
        another: u64,
    },
);

pub fn get_empty_test_wt() -> TestPersistWorkTable
{
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "test/data".to_string(),
    });

    TestPersistWorkTable::new(manager)
}

pub fn get_test_wt() -> TestPersistWorkTable {
    let table = get_empty_test_wt();

    for i in 1..100 {
        let row = TestPersistRow {
            another: i as u64,
            id: i,
        };
        table.insert(row).unwrap();
    }

    table
}

pub fn get_test_wt_without_secondary_indexes() -> TestWithoutSecondaryIndexesWorkTable {
    let manager = Arc::new(DatabaseManager {
        config_path: "tests/data".to_string(),
        database_files_dir: "test/data".to_string(),
    });

    let table = TestWithoutSecondaryIndexesWorkTable::new(manager);

    for i in 1..100 {
        let row = TestWithoutSecondaryIndexesRow {
            another: i as u64,
            id: i,
        };
        table.insert(row).unwrap();
    }

    table
}
