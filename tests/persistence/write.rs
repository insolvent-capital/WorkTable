use crate::{check_if_dirs_are_same, remove_dir_if_exists};

use crate::persistence::{get_test_wt, get_test_wt_without_secondary_indexes};

#[test]
fn test_persist() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/test_persist".to_string()).await;

        let table = get_test_wt().await;
        table.wait_for_ops().await;
        table.persist().await.unwrap();

        assert!(check_if_dirs_are_same(
            "tests/data/test_persist".to_string(),
            "tests/data/expected/test_persist".to_string()
        ))
    });
}

#[test]
fn test_persist_without_secondary_indexes() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/test_without_secondary_indexes".to_string()).await;

        let table = get_test_wt_without_secondary_indexes().await;
        table.wait_for_ops().await;
        table.persist().await.unwrap();

        assert!(check_if_dirs_are_same(
            "tests/data/test_without_secondary_indexes".to_string(),
            "tests/data/expected/test_without_secondary_indexes".to_string()
        ))
    });
}
