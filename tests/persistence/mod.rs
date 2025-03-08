use worktable::prelude::*;
use worktable::worktable;

mod index_page;
mod read;
mod space_index;
mod sync;
mod toc;
mod write;

worktable! (
    name: TestPersist,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: u64,
    },
    indexes: {
        another_idx: another,
    },
    queries: {
        update: {
            AnotherById(another) by id,
        },
        delete: {
             ByAnother() by another,
        }
    }
);

worktable! (
    name: TestWithoutSecondaryIndexes,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: u64,
    },
);

worktable!(
    name: SizeTest,
    columns: {
        id: u32 primary_key,
        number: u64,
    }
);

pub const TEST_ROW_COUNT: usize = 100;

pub fn get_empty_test_wt() -> TestPersistWorkTable {
    let config = PersistenceConfig::new("tests/data", "tests/data").unwrap();
    TestPersistWorkTable::new(config).unwrap()
}

pub fn get_test_wt() -> TestPersistWorkTable {
    let table = get_empty_test_wt();

    for i in 1..100 {
        let row = TestPersistRow { another: i, id: i };
        table.insert(row).unwrap();
    }

    table
}

pub fn get_test_wt_without_secondary_indexes() -> TestWithoutSecondaryIndexesWorkTable {
    let config = PersistenceConfig::new("tests/data", "tests/data").unwrap();
    let table = TestWithoutSecondaryIndexesWorkTable::new(config).unwrap();

    for i in 1..TEST_ROW_COUNT {
        let row = TestWithoutSecondaryIndexesRow {
            another: i as u64,
            id: i as u64,
        };
        table.insert(row).unwrap();
    }

    table
}
