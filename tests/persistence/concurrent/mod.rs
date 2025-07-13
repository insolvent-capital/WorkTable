use std::sync::Arc;
use std::time::Duration;

use tokio::task;
use worktable::prelude::*;
use worktable::worktable;

use crate::remove_dir_if_exists;

worktable! (
    name: TestConcurrent,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        another: u64,
        value: u64,

        field_0: f64,
        field_1: f64,
        field_2: f64,
        field_3: f64,
        field_4: f64,
        field_5: f64,
        field_6: f64,
        field_7: f64,
        field_8: f64,
        field_9: f64,
        field_10: f64,
        field_11: f64,
        field_12: f64,
        field_13: f64,
        field_14: f64,
        field_15: f64,
        field_16: f64,
        field_17: f64,
    },
    indexes: {
        another_idx: another,
        value_idx: value unique,
    },
    queries: {
        update: {
            AnotherById(another) by id,
        },
        delete: {
             ByAnother() by another,
        }
    },
    config: {
        row_derives: Default,
    }
);

#[test]
fn test_concurrent() {
    let config = PersistenceConfig::new("tests/data/concurrent/test", "tests/data/concurrent/test");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/concurrent/test".to_string()).await;
        {
            let table = Arc::new(
                TestConcurrentWorkTable::load_from_file(config.clone())
                    .await
                    .unwrap(),
            );

            let total: u64 = 5_000;
            let tasks = 8;
            let chunk = total / tasks;

            let mut handles = Vec::with_capacity(tasks as usize);
            for t in 0..tasks {
                let start_id = t * chunk;
                let end_id = if t == tasks - 1 {
                    total
                } else {
                    (t + 1) * chunk
                };
                let task_table = table.clone();

                handles.push(task::spawn(async move {
                    for value in start_id..end_id {
                        task_table
                            .insert(TestConcurrentRow {
                                id: task_table.get_next_pk().into(),
                                another: value % 1000,
                                value,
                                ..Default::default()
                            })
                            .unwrap();

                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }))
            }

            // Await all tasks
            for h in handles {
                let _ = h.await;
            }

            table.wait_for_ops().await;
        }
        {
            let table = Arc::new(
                TestConcurrentWorkTable::load_from_file(config.clone())
                    .await
                    .unwrap(),
            );
            assert_eq!(table.count(), 5_000)
        }
    })
}
