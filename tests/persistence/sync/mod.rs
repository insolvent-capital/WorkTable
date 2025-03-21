use crate::persistence::{AnotherByIdQuery, TestPersistRow, TestPersistWorkTable};
use crate::remove_dir_if_exists;
use worktable::prelude::{PersistenceConfig, PrimaryKeyGeneratorState};

mod string_re_read;

#[test]
fn test_space_insert_sync() {
    let config = PersistenceConfig::new("tests/data/sync/insert", "tests/data/sync/insert");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/insert".to_string()).await;

        let pk = {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestPersistRow {
                another: 42,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_insert_many_sync() {
    let config =
        PersistenceConfig::new("tests/data/sync/insert_many", "tests/data/sync/insert_many");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/insert_many".to_string()).await;

        let mut pks = vec![];
        {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            for i in 0..20 {
                let pk = {
                    let row = TestPersistRow {
                        another: i,
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
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
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
    let config =
        PersistenceConfig::new("tests/data/sync/update_full", "tests/data/sync/update_full");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/update_full".to_string()).await;

        let pk = {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestPersistRow {
                another: 42,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update(TestPersistRow {
                    another: 13,
                    id: row.id,
                })
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().another, 13);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_update_query_sync() {
    let config = PersistenceConfig::new(
        "tests/data/sync/update_query",
        "tests/data/sync/update_query",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/update_query".to_string()).await;

        let pk = {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestPersistRow {
                another: 42,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table
                .update_another_by_id(AnotherByIdQuery { another: 13 }, row.id.into())
                .await
                .unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_some());
            assert_eq!(table.select(pk.into()).unwrap().another, 13);
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_sync() {
    let config = PersistenceConfig::new("tests/data/sync/delete", "tests/data/sync/delete");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/delete".to_string()).await;

        let pk = {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestPersistRow {
                another: 42,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.delete(row.id.into()).await.unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}

#[test]
fn test_space_delete_query_sync() {
    let config = PersistenceConfig::new(
        "tests/data/sync/delete_query",
        "tests/data/sync/delete_query",
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/sync/delete_query".to_string()).await;

        let pk = {
            let table = TestPersistWorkTable::load_from_file(config.clone())
                .await
                .unwrap();
            let row = TestPersistRow {
                another: 42,
                id: table.get_next_pk().0,
            };
            table.insert(row.clone()).unwrap();
            table.delete_by_another(row.another).await.unwrap();
            table.wait_for_ops().await;
            row.id
        };
        {
            let table = TestPersistWorkTable::load_from_file(config).await.unwrap();
            assert!(table.select(pk.into()).is_none());
            assert_eq!(table.0.pk_gen.get_state(), pk + 1)
        }
    });
}
