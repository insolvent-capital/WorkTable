use worktable::prelude::*;
use worktable::worktable;

type Arr = [u32; 4];

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: Arr
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
        test: [0; 4],
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
        test: [0; 4],
    };
    let pk = table.insert(row.clone()).unwrap();
    let new_row = TestRow {
        id: 1,
        test: [1; 4],
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, new_row);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test]
async fn update_in_a_middle() {
    let table = TestWorkTable::default();
    for i in 0..10 {
        let row = TestRow {
            id: i,
            test: [0; 4],
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let new_row = TestRow {
        id: 3,
        test: [1; 4],
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(3.into()).unwrap();

    assert_eq!(selected_row, new_row);
}
