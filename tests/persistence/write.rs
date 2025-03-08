use crate::{check_if_dirs_are_same, remove_dir_if_exists};

use crate::persistence::{get_test_wt, get_test_wt_without_secondary_indexes};

#[test]
fn test_persist() {
    remove_dir_if_exists("tests/data/test_persist".to_string());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let table = get_test_wt();
        table.wait_for_ops().await;
        table.persist().unwrap();

        assert!(check_if_dirs_are_same(
            "tests/data/test_persist".to_string(),
            "tests/data/expected/test_persist".to_string()
        ))
    });
}

#[test]
fn test_persist_without_secondary_indexes() {
    remove_dir_if_exists("tests/data/test_without_secondary_indexes".to_string());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let table = get_test_wt_without_secondary_indexes();
        table.wait_for_ops().await;
        table.persist().unwrap();

        assert!(check_if_dirs_are_same(
            "tests/data/test_without_secondary_indexes".to_string(),
            "tests/data/expected/test_without_secondary_indexes".to_string()
        ))
    });
}
