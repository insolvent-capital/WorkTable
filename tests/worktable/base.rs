use std::sync::Arc;
use std::time::Duration;
use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another,
    }
    queries: {
        update: {
            AnotherByExchange(another) by exchange,
            AnotherByTest(another) by test,
            AnotherById(another) by id,
        },
        delete: {
            ByAnother() by another,
            ByExchange() by exchange,
            ByTest() by test,
        }
    }
);

#[test]
fn table_name() {
    let table = TestWorkTable::default();
    let name = table.name();
    assert_eq!(name, "Test");
}

#[test]
fn iter_with() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 3,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    table.iter_with(|_| Ok(())).unwrap()
}

#[tokio::test]
async fn iter_with_async() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 3,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    table
        .iter_with_async(|_| async move { Ok(()) })
        .await
        .unwrap()
}

#[tokio::test]
async fn update_spawn() {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let updated = TestRow {
        id: pk.clone().into(),
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    let shared = table.clone();
    let shared_updated = updated.clone();
    tokio::spawn(async move { shared.update(shared_updated).await })
        .await
        .unwrap()
        .unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, updated);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test]
async fn upsert_spawn() {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let updated = TestRow {
        id: pk.clone().into(),
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    let shared = table.clone();
    let shared_updated = updated.clone();
    tokio::spawn(async move { shared.upsert(shared_updated).await })
        .await
        .unwrap()
        .unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, updated);
    assert!(table.select(2.into()).is_none())
}

#[test]
fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
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
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let updated = TestRow {
        id: pk.clone().into(),
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    table.update(updated.clone()).await.unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, updated);
    assert!(table.select(2.into()).is_none())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_parallel() {
    let table = Arc::new(TestWorkTable::default());
    for i in 1..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: "test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let h = tokio::spawn(async move {
        for i in 1..99 {
            shared
                .update_another_by_test(AnotherByTestQuery { another: i }, (i + 1) as i64)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_micros(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_micros(20)).await;
    for i in 1..99 {
        table
            .update_another_by_id(AnotherByIdQuery { another: i }, i.into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_micros(5)).await;
    }
    h.await.unwrap();
}

#[tokio::test]
async fn delete() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();
    table.delete(pk.clone()).await.unwrap();
    let selected_row = table.select(pk);
    assert!(selected_row.is_none());
    let selected_row = table.select_by_test(1);
    assert!(selected_row.is_none());
    let selected_row = table.select_by_exchange("test".to_string());
    assert!(selected_row.execute().expect("REASON").is_empty());

    let updated = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    let pk = table.insert(updated.clone()).unwrap();
    let new_link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();

    assert_eq!(link, new_link)
}

#[tokio::test]
async fn delete_by_another() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    table.delete_by_another(1).await.unwrap();
    assert_eq!(table.select_all().execute().unwrap().len(), 0)
}

#[tokio::test]
async fn delete_by_exchange() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    table.delete_by_exchange("test".to_string()).await.unwrap();
    assert_eq!(table.select_all().execute().unwrap().len(), 0)
}

#[tokio::test]
async fn delete_by_test() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    table.delete_by_test(2).await.unwrap();
    assert_eq!(table.select_all().execute().unwrap().len(), 1)
}

#[tokio::test]
async fn delete_and_insert_less() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 0,
        another: 0,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test1234567890".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();
    table.delete(pk.clone()).await.unwrap();
    let selected_row = table.select(pk);
    assert!(selected_row.is_none());

    let updated = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 3,
        exchange: "test1".to_string(),
    };
    let pk = table.insert(updated.clone()).unwrap();
    let new_link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();

    assert_ne!(link, new_link)
}

#[tokio::test]
async fn delete_and_replace() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 0,
        another: 0,
        exchange: "test1".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();
    table.delete(pk.clone()).await.unwrap();
    let selected_row = table.select(pk);
    assert!(selected_row.is_none());

    let updated = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    let pk = table.insert(updated.clone()).unwrap();
    let new_link = table.0.pk_map.get(&pk).map(|kv| kv.get().value).unwrap();

    assert_eq!(link, new_link)
}

#[tokio::test]
async fn upsert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    table.upsert(row.clone()).await.unwrap();
    let updated = TestRow {
        id: row.id,
        test: 2,
        another: 3,
        exchange: "test".to_string(),
    };
    table.upsert(updated.clone()).await.unwrap();
    let selected_row = table.select(row.id.into()).unwrap();

    assert_eq!(selected_row, updated);
    assert!(table.select(2.into()).is_none())
}

#[test]
fn insert_same() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let res = table.insert(row.clone());
    assert!(res.is_err())
}

#[test]
fn insert_exchange_same() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let res = table.insert(row.clone());
    assert!(res.is_err())
}

#[test]
fn select_by_exchange() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let selected_rows = table
        .select_by_exchange("test".to_string())
        .execute()
        .expect("rows");

    assert_eq!(selected_rows.len(), 1);
    assert!(selected_rows.contains(&row));
    assert!(table
        .select_by_exchange("test1".to_string())
        .execute()
        .expect("REASON")
        .is_empty())
}

#[test]
fn select_multiple_by_exchange() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let row_next = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row_next.clone()).unwrap();
    let selected_rows = table
        .select_by_exchange("test".to_string())
        .execute()
        .expect("rows");

    assert_eq!(selected_rows.len(), 2);
    assert!(selected_rows.contains(&row));
    assert!(selected_rows.contains(&row_next));
    assert!(table
        .select_by_exchange("test1".to_string())
        .execute()
        .expect("REASON")
        .is_empty())
}

#[test]
fn select_by_test() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();
    let selected_row = table.select_by_test(1).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select_by_test(2).is_none())
}

#[test]
fn select_all_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(&all[0], &row1);
    assert_eq!(&all[1], &row2)
}

#[test]
fn select_all_limit_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 100 - 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 100 - 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: 100 - i,
            another: 1,
            exchange: "test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table.select_all().limit(2).execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(&all[0], &row1);
    assert_eq!(&all[1], &row2)
}

#[test]
fn select_all_offset_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 100 - 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 100 - 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();

    let all = table.select_all().offset(1).execute().unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(&all[0], &row2);

    let all = table.select_all().offset(2).execute().unwrap();
    assert_eq!(all.len(), 0);
}

#[test]
fn select_all_order_by_unique_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: "test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table
        .select_all()
        .order_by(Order::Asc, "test")
        .limit(2)
        .execute()
        .unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(&all[0].test, &1);
    assert_eq!(&all[1].test, &2)
}

#[test]
fn select_all_order_by_non_unique_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 3,
        exchange: "c_test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "b_test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: "a_test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table
        .select_all()
        .order_by(Order::Asc, "exchange")
        .limit(2)
        .execute()
        .unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(&all[0].exchange, &"a_test".to_string());
    assert_eq!(&all[1].exchange, &"a_test".to_string())
}

#[test]
fn select_all_order_two_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 3,
        exchange: "a_test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "b_test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: "c_test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table
        .select_all()
        .order_by(Order::Asc, "test")
        .limit(3)
        .execute()
        .unwrap();

    assert_eq!(all.len(), 3);
    assert_eq!(&all[0].exchange, &"a_test".to_string());
    assert_eq!(&all[1].exchange, &"b_test".to_string());
    assert_eq!(&all[2].exchange, &"c_test".to_string());
    assert_eq!(&all[2].test, &3)
}

#[test]
fn select_by_order_by_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 3,
        exchange: "a_test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "b_test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: "c_test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table
        .select_by_exchange("c_test".to_string())
        .order_by(Order::Desc, "test")
        .limit(3)
        .execute()
        .expect("rows");

    assert_eq!(all.len(), 3);
    assert_eq!(&all[0].exchange, &"c_test".to_string());
    assert_eq!(&all[0].test, &99);
    assert_eq!(&all[1].exchange, &"c_test".to_string());
    assert_eq!(&all[1].test, &98);
    assert_eq!(&all[2].exchange, &"c_test".to_string());
    assert_eq!(&all[2].test, &97)
}

#[test]
fn select_by_offset_test() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 3,
        exchange: "a_test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 2,
        exchange: "b_test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();
    for i in 3..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i,
            another: 1,
            exchange: "c_test".to_string(),
        };
        let _ = table.insert(row.clone()).unwrap();
    }

    let all = table
        .select_by_exchange("c_test".to_string())
        .order_by(Order::Desc, "test")
        .offset(10)
        .limit(3)
        .execute()
        .expect("rows");

    assert_eq!(all.len(), 3);
    assert_eq!(&all[0].exchange, &"c_test".to_string());
    assert_eq!(&all[0].test, &89);
    assert_eq!(&all[1].exchange, &"c_test".to_string());
    assert_eq!(&all[1].test, &88);
    assert_eq!(&all[2].exchange, &"c_test".to_string());
    assert_eq!(&all[2].test, &87)
}

#[tokio::test]
async fn test_update_by_non_unique() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row1.clone()).unwrap();
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row2.clone()).unwrap();

    let row = AnotherByExchangeQuery { another: 3 };
    table
        .update_another_by_exchange(row, "test".to_string())
        .await
        .unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestRow {
            id: 0,
            test: 1,
            another: 3,
            exchange: "test".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestRow {
            id: 1,
            test: 2,
            another: 3,
            exchange: "test".to_string(),
        }
    )
}

#[tokio::test]
async fn test_update_by_unique() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    let row = AnotherByTestQuery { another: 3 };
    table.update_another_by_test(row, 1).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 3,
            exchange: "test".to_string(),
        }
    )
}

#[tokio::test]
async fn test_update_by_pk() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();

    let row = AnotherByIdQuery { another: 3 };
    table.update_another_by_id(row, pk).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 3,
            exchange: "test".to_string(),
        }
    )
}

//#[test]
fn _bench() {
    let table = TestWorkTable::default();

    let mut v = Vec::with_capacity(10000);

    for i in 0..10000 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: "XD".to_string(),
        };

        let a = table.insert(row).expect("TODO: panic message");
        v.push(a)
    }

    for a in v {
        table.select(a).expect("TODO: panic message");
    }
}
