use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String,
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another,
    }
    queries: {
        update: {
            ExchangeByTest(exchange) by test,
            ExchangeById(exchange) by id,
            ExchangeByAbother(exchange) by another,
        }
    }
);

#[tokio::test]
async fn test_update_string_full_row() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    table
        .update(TestRow {
            id: row.id,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        })
        .await
        .unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_unique() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByTestQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_test(row, 1).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_pk() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByIdQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_id(row, pk).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_string_by_non_unique() {
    let table = TestWorkTable::default();
    let row1 = TestRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeByAbotherQuery {
        exchange: "bigger test to test string update".to_string(),
    };
    table.update_exchange_by_abother(row, 1).await.unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestRow {
            id: 1,
            test: 2,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 2);
    assert!(empty_links.contains(&first_link));
    assert!(empty_links.contains(&second_link))
}

#[tokio::test]
async fn update_many_times() {
    let table = TestWorkTable::default();
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let mut i_state = HashMap::new();
    for _ in 0..1000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_exchange_by_id(
                ExchangeByIdQuery {
                    exchange: format!("test_{val}"),
                },
                id_to_update.into(),
            )
            .await
            .unwrap();
        {
            i_state
                .entry(id_to_update as i64 + 1)
                .and_modify(|v| *v = format!("test_{val}"))
                .or_insert(format!("test_{val}"));
        }
    }

    for (test, val) in i_state {
        let row = table.select_by_test(test).unwrap();
        assert_eq!(row.exchange, val)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_parallel() {
    let table = Arc::new(TestWorkTable::default());
    let i_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            test: i + 1,
            another: 1,
            exchange: format!("test_{i}"),
        };
        let _ = table.insert(row.clone()).unwrap();
    }
    let shared = table.clone();
    let shared_i_state = i_state.clone();
    let h = tokio::spawn(async move {
        for _ in 0..1000 {
            let val = fastrand::u64(..);
            let id_to_update = fastrand::i64(1..=100);
            shared
                .update_exchange_by_test(
                    ExchangeByTestQuery {
                        exchange: format!("test_{val}"),
                    },
                    id_to_update,
                )
                .await
                .unwrap();
            {
                let mut guard = shared_i_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = format!("test_{val}"))
                    .or_insert(format!("test_{val}"));
            }
            tokio::time::sleep(Duration::from_micros(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_micros(20)).await;
    for _ in 0..1000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_exchange_by_id(
                ExchangeByIdQuery {
                    exchange: format!("test_{val}"),
                },
                id_to_update.into(),
            )
            .await
            .unwrap();
        {
            let mut guard = i_state.lock();
            guard
                .entry(id_to_update as i64 + 1)
                .and_modify(|v| *v = format!("test_{val}"))
                .or_insert(format!("test_{val}"));
        }
        tokio::time::sleep(Duration::from_micros(5)).await;
    }
    h.await.unwrap();

    for (test, val) in i_state.lock_arc().iter() {
        let row = table.select_by_test(*test).unwrap();
        assert_eq!(&row.exchange, val)
    }
}

worktable! (
    name: TestMoreStrings,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: u64,
        exchange: String,
        some_string: String,
        other_srting: String,
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another,
    }
    queries: {
        update: {
            ExchangeAndSomeByTest(exchange, some_string) by test,
            ExchangeAndSomeById(exchange, some_string) by id,
            ExchangeAndSomeByAnother(exchange, some_string) by another,
            SomeOtherByExchange(some_string, other_srting) by exchange,
        }
    }
);

#[tokio::test]
async fn test_update_many_strings_by_unique() {
    let table = TestMoreStringsWorkTable::default();
    let row = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByTestQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_exchange_and_some_by_test(row, 1)
        .await
        .unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_many_strings_by_pk() {
    let table = TestMoreStringsWorkTable::default();
    let row = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByIdQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table.update_exchange_and_some_by_id(row, pk).await.unwrap();

    let row = table.select_by_test(1).unwrap();

    assert_eq!(
        row,
        TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(table.0.data.get_empty_links().first().unwrap(), &first_link)
}

#[tokio::test]
async fn test_update_many_strings_by_non_unique() {
    let table = TestMoreStringsWorkTable::default();
    let row1 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test another".to_string(),
        some_string: "some".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = ExchangeAndSomeByAnotherQuery {
        exchange: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_exchange_and_some_by_another(row, 1)
        .await
        .unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestMoreStringsRow {
            id: 1,
            test: 2,
            another: 1,
            exchange: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            other_srting: "other".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 2);
    assert!(empty_links.contains(&first_link));
    assert!(empty_links.contains(&second_link))
}

#[tokio::test]
async fn test_update_many_strings_by_string() {
    let table = TestMoreStringsWorkTable::default();
    let row1 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 1,
        exchange: "test".to_string(),
        some_string: "something".to_string(),
        other_srting: "other er".to_string(),
    };
    let pk = table.insert(row1.clone()).unwrap();
    let first_link = table.0.pk_map.get(&pk).unwrap().get().value;
    let row2 = TestMoreStringsRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 1,
        exchange: "test".to_string(),
        some_string: "some ome".to_string(),
        other_srting: "other".to_string(),
    };
    let pk = table.insert(row2.clone()).unwrap();
    let second_link = table.0.pk_map.get(&pk).unwrap().get().value;

    let row = SomeOtherByExchangeQuery {
        other_srting: "bigger test to test string update".to_string(),
        some_string: "some bigger some to test".to_string(),
    };
    table
        .update_some_other_by_exchange(row, "test".to_string())
        .await
        .unwrap();

    let all = table.select_all().execute().unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(
        &all[0],
        &TestMoreStringsRow {
            id: 0,
            test: 1,
            another: 1,
            other_srting: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            exchange: "test".to_string(),
        }
    );
    assert_eq!(
        &all[1],
        &TestMoreStringsRow {
            id: 1,
            test: 2,
            another: 1,
            other_srting: "bigger test to test string update".to_string(),
            some_string: "some bigger some to test".to_string(),
            exchange: "test".to_string(),
        }
    );
    let empty_links = table.0.data.get_empty_links();
    assert_eq!(empty_links.len(), 2);
    assert!(empty_links.contains(&first_link));
    assert!(empty_links.contains(&second_link))
}
