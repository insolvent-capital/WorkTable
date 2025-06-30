use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestSync,
    persist: true,
    columns: {
        id: String primary_key,
        another: u64,
        non_unique: u32,
        field: f64,
    },
    indexes: {
        another_idx: another unique,
        non_unique_idx: non_unique
    },
    queries: {
        update: {
            AnotherById(another) by id,
            FieldByAnother(field) by another,
            AnotherByNonUnique(another) by non_unique
        },
        delete: {
             ByAnother() by another,
        }
    }
);

#[test]
fn test_space_insert_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/insert",
        "tests/data/unsized_primary_sync/insert",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/insert".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.234,
                id: "Some string to test".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
        }
    });
}

#[test]
fn test_space_insert_many_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/insert_many",
        "tests/data/unsized_primary_sync/insert_many",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/insert_many".to_string()).await;

        let mut pks = vec![];
        {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for i in 0..1_000 {
                let pk = {
                    let row = TestSyncRow {
                        another: i,
                        non_unique: (i % 4) as u32,
                        field: i as f64 / 100.0,
                        id: format!("Some string to test number {i}"),
                    };
                    table.insert(row.clone()).unwrap();
                    row.id
                };
                pks.push(pk);
            }
            table.wait_for_ops().await;
        }

        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            for pk in pks {
                assert!(table.select(pk.into()).is_some());
            }
        }
    });
}

#[test]
fn test_space_update_full_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/update_full",
        "tests/data/unsized_primary_sync/update_full",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/update_full".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table
                .update(TestSyncRow {
                    another: 13,
                    non_unique: 0,
                    field: 0.0,
                    id: "Some string before".to_string(),
                })
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.clone().into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().another, 13);
        }
    });
}

#[test]
fn test_space_update_query_pk_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/update_query_pk",
        "tests/data/unsized_primary_sync/update_query_pk",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/update_query_pk".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table
                .update_another_by_id(AnotherByIdQuery { another: 13 }, row.id.clone().into())
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.clone().into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().another, 13);
        }
    });
}

#[test]
fn test_space_update_query_unique_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/update_query_unique",
        "tests/data/unsized_primary_sync/update_query_unique",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/update_query_unique".to_string())
            .await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table
                .update_field_by_another(FieldByAnotherQuery { field: 1.0 }, 42)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.clone().into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().field, 1.0);
        }
    });
}

#[test]
fn test_space_update_query_non_unique_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/update_query_non_unique",
        "tests/data/unsized_primary_sync/update_query_non_unique",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/update_query_non_unique".to_string())
            .await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 10,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table
                .update_another_by_non_unique(AnotherByNonUniqueQuery { another: 13 }, 10)
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.clone().into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().another, 13);
        }
    });
}

#[test]
fn test_space_delete_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/delete",
        "tests/data/unsized_primary_sync/delete",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/delete".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            let another_row = TestSyncRow {
                another: 43,
                non_unique: 0,
                field: 0.0,
                id: "Some string".to_string(),
            };
            table.insert(another_row.clone()).unwrap();
            table.delete(another_row.id.clone().into()).await.unwrap();
            table.wait_for_ops().await;
            another_row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
        }
    });
}

#[test]
fn test_space_delete_query_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_primary_sync/delete_query",
        "tests/data/unsized_primary_sync/delete_query",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_primary_sync/delete_query".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: 42,
                non_unique: 0,
                field: 0.0,
                id: "Some string before".to_string(),
            };
            table.insert(row.clone()).unwrap();
            table.delete_by_another(row.another).await.unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
        }
    });
}
