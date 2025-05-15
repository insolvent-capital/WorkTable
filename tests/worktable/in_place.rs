use std::sync::Arc;

use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        val1: u64,
        val2: i16,
    },
    queries: {
        in_place: {
            ValById(val) by id,
            Val2ById(val2) by id,
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
async fn test_update_val_by_id_multi_thread() -> eyre::Result<()> {
    let table = Arc::new(TestWorkTable::default());
    let row = TestRow {
        id: table.get_next_pk().0,
        val: 0,
        val1: 0,
        val2: 0,
    };
    let pk = table.insert(row)?;
    let shared_table = table.clone();
    let h = tokio::spawn(async move {
        for _ in 0..10000 {
            shared_table
                .update_val_by_id_in_place(|val| *val += 1, pk.0)
                .await
                .unwrap()
        }
    });
    for _ in 0..10000 {
        table
            .update_val_by_id_in_place(|val| *val += 1, pk.0)
            .await?
    }
    h.await?;
    let row = table.select(pk).unwrap();
    assert_eq!(row.val, 20000);
    Ok(())
}
