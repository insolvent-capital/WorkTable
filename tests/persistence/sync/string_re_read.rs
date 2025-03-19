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
                    first: "first_again".to_string(),
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
