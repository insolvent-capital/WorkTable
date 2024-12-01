use worktable::prelude::*;
use worktable::worktable;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Clone, Copy, Debug, Deserialize, Serialize, PartialEq, PartialOrd)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(Debug))]
pub enum SomeEnum {
    First,
    Second,
    Third,
}

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: SomeEnum
    },
    queries: {
        update: {
            Test(test) by id,
        }
    }
);

#[test]
fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: 1,
        test: SomeEnum::First,
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test]
async fn update() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: 1,
        test: SomeEnum::First,
    };
    let pk = table.insert(row.clone()).unwrap();
    let updated = TestRow {
        id: 1,
        test: SomeEnum::Second,
    };
    table.update(updated.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, updated);
    assert!(table.select(2.into()).is_none())
}
