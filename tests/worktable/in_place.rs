use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        val1: u64,
        val2: i16,
        another: String,
        something: u64,
    },
    queries: {
        in_place: {
            ValById(val) by id,
            Val2ById(val2) by id,
        }
        update: {
            AnotherById(another) by id,
            SomethingById(something) by id,
        }
    }
);

#[tokio::test]
async fn test_update_val_by_id() -> eyre::Result<()> {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
        another: "another".to_string(),
        something: 0,
    };
    let pk = table.insert(row)?;
    for _ in 0..10000 {
        table
            .update_val_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    let row = table.select(pk).unwrap();
    assert_eq!(row.val, 10000);
    Ok(())
}

#[tokio::test]
async fn test_update_val2_by_id() -> eyre::Result<()> {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
        another: "another".to_string(),
        something: 0,
    };
    let pk = table.insert(row)?;
    for _ in 0..100 {
        table
            .update_val_2_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    let row = table.select(pk).unwrap();
    assert_eq!(row.val2, 100);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_val_by_id_two_thread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
        another: "another".to_string(),
        something: 0,
    };
    let pk = table.insert(row)?;
    let shared_table = table.clone();
    let h = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    for _ in 0..10_000 {
        table
            .update_val_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    h.await?;
    let row = table.select(pk).unwrap();
    assert_eq!(row.val, 20_000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_val_and_val2_by_id_four_thread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
        another: "another".to_string(),
        something: 0,
    };
    let pk = table.insert(row)?;
    let shared_table = table.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    let shared_table = table.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_2_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    let shared_table = table.clone();
    let h3 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    for _ in 0..10_000 {
        table
            .update_val_2_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    h1.await?;
    h2.await?;
    h3.await?;
    let row = table.select(pk).unwrap();
    assert_eq!(row.val, 20_000);
    assert_eq!(row.val2, 20_000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_val_by_id_four_thread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
        another: "another".to_string(),
        something: 0,
    };
    let pk = table.insert(row)?;
    let shared_table = table.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    let shared_table = table.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    let shared_table = table.clone();
    let h3 = tokio::spawn(async move {
        for _ in 0..10_000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    for _ in 0..10_000 {
        table
            .update_val_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    h1.await?;
    h2.await?;
    h3.await?;
    let row = table.select(pk).unwrap();
    assert_eq!(row.val, 40_000);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_update_in_place_and_update_sized_multithread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let i_state = Arc::new(Mutex::new(HashMap::new()));
    let val_state = Arc::new(Mutex::new(HashMap::new()));
    let val2_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            val: 0,
            val1: 0,
            val2: 0,
            another: format!("another_{i}"),
            something: 0,
        };
        let _ = table.insert(row.clone())?;
    }
    let shared = table.clone();
    let shared_val_state = val_state.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..15_000 {
            let val = fastrand::i64(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_val_by_id_in_place(|v| *v = val.into(), id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_val_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    let shared = table.clone();
    let shared_val2_state = val2_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..15_000 {
            let val = fastrand::i16(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_val_2_by_id_in_place(|v| *v = val.into(), id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_val2_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    tokio::time::sleep(Duration::from_micros(20)).await;
    for _ in 0..5000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_something_by_id(SomethingByIdQuery { something: val }, id_to_update.into())
            .await?;
        {
            let mut guard = i_state.lock();
            guard
                .entry(id_to_update)
                .and_modify(|v| *v = val)
                .or_insert(val);
        }
    }
    h1.await?;
    h2.await?;

    for (id, smth) in i_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        assert_eq!(&row.something, smth);
    }
    for (id, val) in val2_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        assert_eq!(&row.val2, val);
    }
    for (id, val) in val_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        assert_eq!(&row.val, val);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_update_in_place_and_update_unsized_multithread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let i_state = Arc::new(Mutex::new(HashMap::new()));
    let val_state = Arc::new(Mutex::new(HashMap::new()));
    let val2_state = Arc::new(Mutex::new(HashMap::new()));
    for i in 0..100 {
        let row = TestRow {
            id: table.get_next_pk().into(),
            val: 0,
            val1: 0,
            val2: 0,
            another: format!("another_{i}"),
            something: 0,
        };
        let _ = table.insert(row.clone())?;
    }
    let shared = table.clone();
    let shared_val_state = val_state.clone();
    let h1 = tokio::spawn(async move {
        for _ in 0..15_000 {
            let val = fastrand::i64(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_val_by_id_in_place(|v| *v = val.into(), id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_val_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    let shared = table.clone();
    let shared_val2_state = val2_state.clone();
    let h2 = tokio::spawn(async move {
        for _ in 0..15_000 {
            let val = fastrand::i16(..);
            let id_to_update = fastrand::u64(0..=99);
            shared
                .update_val_2_by_id_in_place(|v| *v = val.into(), id_to_update)
                .await
                .unwrap();
            {
                let mut guard = shared_val2_state.lock();
                guard
                    .entry(id_to_update)
                    .and_modify(|v| *v = val)
                    .or_insert(val);
            }
        }
    });
    tokio::time::sleep(Duration::from_micros(20)).await;
    for _ in 0..5000 {
        let val = fastrand::u64(..);
        let id_to_update = fastrand::u64(0..=99);
        table
            .update_another_by_id(
                AnotherByIdQuery {
                    another: format!("another_{val}"),
                },
                id_to_update.into(),
            )
            .await?;
        {
            let mut guard = i_state.lock();
            guard
                .entry(id_to_update)
                .and_modify(|v| *v = format!("another_{val}"))
                .or_insert(format!("another_{val}"));
        }
    }
    h1.await?;
    h2.await?;

    for (id, smth) in i_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        assert_eq!(&row.another, smth);
    }
    let mut errors = 0;
    for (id, val) in val2_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        if &row.val2 != val {
            errors += 1;
        }
    }
    assert_eq!(errors, 0);
    let mut errors = 0;
    for (id, val) in val_state.lock_arc().iter() {
        let row = table.select((*id).into()).unwrap();
        if &row.val != val {
            errors += 1;
        }
    }
    assert_eq!(errors, 0);
    Ok(())
}
