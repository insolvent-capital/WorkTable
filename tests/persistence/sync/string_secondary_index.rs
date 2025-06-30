use worktable::prelude::*;
use worktable_codegen::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestSync,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: String,
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
        "tests/data/unsized_secondary_sync/insert",
        "tests/data/unsized_secondary_sync/insert",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/insert".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string to test".to_string(),
                non_unique: 0,
                field: 0.234,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_insert_many_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/insert_many",
        "tests/data/unsized_secondary_sync/insert_many",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/insert_many".to_string()).await;

        let mut pks = vec![];
        {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for i in 0..20 {
                let pk = {
                    let row = TestSyncRow {
                        another: format!("Some string to test number {i}"),
                        non_unique: (i % 4) as u32,
                        field: i as f64 / 100.0,
                        id: table.get_next_pk().0,
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
            let last = *pks.last().unwrap();
            for pk in pks {
                assert!(table.select(pk.into()).is_some());
            }
            assert_eq!(table.0.pk_gen.get_state(), last + 1)
        }
    });
}

#[test]
fn test_space_update_full_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/update_full",
        "tests/data/unsized_secondary_sync/update_full",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_full".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update(TestSyncRow {
                    another: "Some string to test updated".to_string(),
                    non_unique: 0,
                    field: 0.0,
                    id: row.id,
                })
                .await
                .unwrap();
            table.wait_for_ops().await;
            assert_eq!(
                table.select(row.id.into()).unwrap().another,
                "Some string to test updated".to_string()
            );
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(
                table.select(pk.into()).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_pk_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/update_query_pk",
        "tests/data/unsized_secondary_sync/update_query_pk",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_query_pk".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update_another_by_id(
                    AnotherByIdQuery {
                        another: "Some string to test updated".to_string(),
                    },
                    row.id.into(),
                )
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(
                table.select(pk.into()).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_unique_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/update_query_unique",
        "tests/data/unsized_secondary_sync/update_query_unique",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/update_query_unique".to_string())
            .await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update_field_by_another(
                    FieldByAnotherQuery { field: 1.0 },
                    "Some string before".to_string(),
                )
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().field, 1.0);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_non_unique_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/update_query_non_unique",
        "tests/data/unsized_secondary_sync/update_query_non_unique",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists(
            "tests/data/unsized_secondary_sync/update_query_non_unique".to_string(),
        )
        .await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 10,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update_another_by_non_unique(
                    AnotherByNonUniqueQuery {
                        another: "Some string to test updated".to_string(),
                    },
                    10,
                )
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(
                table.select(pk.into()).unwrap().another,
                "Some string to test updated".to_string()
            );
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/delete",
        "tests/data/unsized_secondary_sync/delete",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/delete".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.delete(row.id.into()).await.unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_query_sync() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/delete_query",
        "tests/data/unsized_secondary_sync/delete_query",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/delete_query".to_string()).await;

        let pk = {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestSyncRow {
                another: "Some string before".to_string(),
                non_unique: 0,
                field: 0.0,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.delete_by_another(row.another).await.unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_all_data_is_available() {
    let config = PersistenceConfig::new(
        "tests/data/unsized_secondary_sync/data_is_available",
        "tests/data/unsized_secondary_sync/data_is_available",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/unsized_secondary_sync/data_is_available".to_string())
            .await;

        {
            let table = TestSyncWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for i in 0..2000 {
                let row = TestSyncRow {
                    another: format!("ValueNumber{i}"),
                    non_unique: i % 200,
                    field: 0.0,
                    id: table.get_next_pk().0,
                };
                table.insert(row.clone()).unwrap();
            }

            table.wait_for_ops().await;
        };
        {
            let table = TestSyncWorkTable::load_from_file(config).await.unwrap();
            for i in 0..2000 {
                assert!(table.select_by_another(format!("ValueNumber{i}")).is_some());
            }
            for i in 0..200 {
                assert_eq!(table.select_by_non_unique(i).execute().unwrap().len(), 10,);
            }
        }
    });
}
