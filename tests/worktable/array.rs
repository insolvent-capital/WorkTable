use worktable::prelude::*;
use worktable::worktable;

type Arr = [u8; 20];

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: Arr
    },
    queries: {
        update: {
            TestById(test) by id,
        }
    }
);

#[test]
fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: 1,
        test: [1; 20],
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
        test: [1; 20],
    };
    let pk = table.insert(row.clone()).unwrap();
    let new_row = TestRow {
        id: 1,
        test: [2; 20],
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
            test: [1; 20],
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let new_row = TestRow {
        id: 3,
        test: [1; 20],
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(3.into()).unwrap();

    assert_eq!(selected_row, new_row);
}

#[tokio::test]
async fn update_query() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: 1,
        test: [1; 20],
    };
    let pk = table.insert(row.clone()).unwrap();
    let q = TestByIdQuery { test: [2; 20] };
    table
        .update_test_by_id(q.clone(), pk.clone())
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row.test, q.test);
    assert!(table.select(2.into()).is_none())
}

type ArrI = [i64; 20];

worktable! (
    name: TestI,
    columns: {
        id: u64 primary_key autoincrement,
        test: ArrI
    },
    queries: {
        update: {
            TestIById(test) by id,
        }
    }
);

#[test]
fn insert_i() {
    let table = TestIWorkTable::default();
    let row = TestIRow {
        id: 1,
        test: [1; 20],
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test]
async fn update_i() {
    let table = TestIWorkTable::default();
    let row = TestIRow {
        id: 1,
        test: [1; 20],
    };
    let pk = table.insert(row.clone()).unwrap();
    let new_row = TestIRow {
        id: 1,
        test: [2; 20],
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, new_row);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test]
async fn update_in_a_middle_i() {
    let table = TestIWorkTable::default();
    for i in 0..10 {
        let row = TestIRow {
            id: i,
            test: [1; 20],
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let new_row = TestIRow {
        id: 3,
        test: [1; 20],
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(3.into()).unwrap();

    assert_eq!(selected_row, new_row);
}

#[tokio::test]
async fn update_query_i() {
    let table = TestIWorkTable::default();
    let row = TestIRow {
        id: 1,
        test: [1; 20],
    };
    let pk = table.insert(row.clone()).unwrap();
    let q = TestIByIdQuery { test: [2; 20] };
    table
        .update_test_i_by_id(q.clone(), pk.clone())
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row.test, q.test);
    assert!(table.select(2.into()).is_none())
}
