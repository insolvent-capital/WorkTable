use crate::remove_dir_if_exists;

use worktable::prelude::*;
use worktable_codegen::worktable;

worktable!(
    name: StringReRead,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        first: String,
        second: String,
        third: String,
        last: String,
    },
    indexes: {
        first_idx: first,
        second_idx: second unique,
    },
);

#[test]
fn test_key() {
    let config = PersistenceConfig::new("tests/data/key", "tests/data/key");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key".to_string()).await;

        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third".to_string(),
                    second: "second".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            table
                .insert(StringReReadRow {
                    first: "first".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_again".to_string(),
                    second: "second_again".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();

            table.wait_for_ops().await
        }
        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            table
                .insert(StringReReadRow {
                    first: "first_last".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_last".to_string(),
                    second: "second_last".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            table.wait_for_ops().await
        }
        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 3);
        }
    })
}

#[test]
fn test_big_amount_reread() {
    let config = PersistenceConfig::new("tests/data/key_big_amount", "tests/data/key_big_amount");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/key_big_amount".to_string()).await;

        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for i in 0..1000 {
                table
                    .insert(StringReReadRow {
                        first: format!("first_{}", i % 100),
                        id: table.get_next_pk().into(),
                        third: format!("third_{}", i),
                        second: format!("second_{}", i),
                        last: format!("_________________________last_____________________{}", i),
                    })
                    .unwrap();
            }

            table.wait_for_ops().await
        }
        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            table
                .insert(StringReReadRow {
                    first: "first_last".to_string(),
                    id: table.get_next_pk().into(),
                    third: "third_last".to_string(),
                    second: "second_last".to_string(),
                    last: "_________________________last_____________________".to_string(),
                })
                .unwrap();
            table.wait_for_ops().await
        }
        {
            let table = StringReReadWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 1001);
        }
    })
}
