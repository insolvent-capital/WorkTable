use crate::remove_dir_if_exists;
use uuid::Uuid;

use worktable::prelude::*;
use worktable_codegen::worktable;

worktable!(
    name: UuidReRead,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        first: Uuid,
        second: Uuid,
    },
    indexes: {
        first_idx: first,
        second_idx: second unique,
    },
);

#[test]
fn test_uuid() {
    let config = PersistenceConfig::new("tests/data/uuid/reread", "tests/data/uuid/reread");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/uuid/reread".to_string()).await;

        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            table
                .insert(UuidReReadRow {
                    first: Uuid::now_v7(),
                    id: table.get_next_pk().into(),
                    second: Uuid::now_v7(),
                })
                .unwrap();
            table
                .insert(UuidReReadRow {
                    first: Uuid::now_v7(),
                    id: table.get_next_pk().into(),
                    second: Uuid::now_v7(),
                })
                .unwrap();

            table.wait_for_ops().await
        }
        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            table
                .insert(UuidReReadRow {
                    first: Uuid::now_v7(),
                    id: table.get_next_pk().into(),
                    second: Uuid::now_v7(),
                })
                .unwrap();
            table.wait_for_ops().await
        }
        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 3);
        }
    })
}

#[test]
fn test_big_amount_reread() {
    let config = PersistenceConfig::new("tests/data/uuid/big_amount", "tests/data/uuid/big_amount");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/uuid/big_amount".to_string()).await;

        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for _ in 0..1000 {
                table
                    .insert(UuidReReadRow {
                        first: Uuid::now_v7(),
                        id: table.get_next_pk().into(),
                        second: Uuid::now_v7(),
                    })
                    .unwrap();
            }

            table.wait_for_ops().await
        }
        let second_last = Uuid::now_v7();
        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();

            table
                .insert(UuidReReadRow {
                    first: Uuid::now_v7(),
                    id: table.get_next_pk().into(),
                    second: second_last,
                })
                .unwrap();
            table.wait_for_ops().await
        }
        {
            let table = UuidReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1001);
            assert!(table.select_by_second(second_last).is_some());
        }
    })
}
