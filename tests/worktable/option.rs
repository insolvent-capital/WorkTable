use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: u64 optional,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            TestById(test) by id,
            TestByAnother(test) by another,
            TestByExchange(test) by exchange,
        }
    }
);

#[tokio::test]
async fn update() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let new_row = TestRow {
        id: pk.clone().into(),
        test: Some(1),
        another: 1,
        exchange: 1,
    };
    table.update(new_row.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row, new_row);
}

#[tokio::test]
async fn update_by_another() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    table
        .update_test_by_another(TestByAnotherQuery { test: Some(1) }, 1)
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(1));
}

#[tokio::test]
async fn update_by_exchange() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: None,
        another: 1,
        exchange: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    table
        .update_test_by_exchange(TestByExchangeQuery { test: Some(1) }, 1)
        .await
        .unwrap();
    let selected_row = table.select(pk).unwrap();
    assert_eq!(selected_row.test, Some(1));
}
