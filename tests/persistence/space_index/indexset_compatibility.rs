mod sized {
    use std::fs::copy;

    use data_bucket::{Link, INNER_PAGE_SIZE};
    use indexset::concurrent::map::BTreeMap;
    use worktable::prelude::{SpaceIndex, SpaceIndexOps};

    use crate::{check_if_files_are_same, remove_file_if_exists};

    #[tokio::test]
    async fn test_indexset_node_creation() {
        remove_file_if_exists(
            "tests/data/space_index/indexset/process_create_node.wt.idx".to_string(),
        )
        .await;

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/indexset/process_create_node.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = BTreeMap::<u32, Link>::new();
        let (_, cdc) = indexset.insert_cdc(
            5,
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24,
            },
        );
        for event in cdc {
            space_index.process_change_event(event).await.unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index/indexset/process_create_node.wt.idx".to_string(),
            "tests/data/expected/space_index/indexset/process_create_node.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at() {
        remove_file_if_exists(
            "tests/data/space_index/indexset/process_insert_at.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/indexset/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/indexset/process_insert_at.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = space_index.parse_indexset().await.unwrap();
        let (_, cdc) = indexset.insert_cdc(
            3,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48,
            },
        );
        for event in cdc {
            space_index.process_change_event(event).await.unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index/indexset/process_insert_at.wt.idx".to_string(),
            "tests/data/expected/space_index/indexset/process_insert_at.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at_big_amount() {
        remove_file_if_exists(
            "tests/data/space_index/indexset/process_insert_at_big_amount.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/indexset/process_insert_at_big_amount.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/indexset/process_insert_at_big_amount.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = space_index.parse_indexset().await.unwrap();

        let (_, cdc) = indexset.insert_cdc(
            1000,
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 24,
            },
        );
        for event in cdc {
            space_index.process_change_event(event).await.unwrap();
        }

        for i in (6..911).rev() {
            let (_, cdc) = indexset.insert_cdc(
                i,
                Link {
                    page_id: 0.into(),
                    offset: i * 24,
                    length: 24,
                },
            );
            for event in cdc {
                space_index.process_change_event(event).await.unwrap();
            }
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index/indexset/process_insert_at_big_amount.wt.idx".to_string(),
            "tests/data/expected/space_index/indexset/process_insert_at_big_amount.wt.idx"
                .to_string()
        ))
    }
}

mod unsized_ {
    use std::fs::copy;

    use crate::{check_if_files_are_same, remove_file_if_exists};
    use data_bucket::{Link, INNER_PAGE_SIZE};
    use indexset::concurrent::map::BTreeMap;
    use indexset::core::pair::Pair;
    use worktable::prelude::{SpaceIndexOps, SpaceIndexUnsized};
    use worktable::UnsizedNode;

    #[tokio::test]
    async fn test_indexset_node_creation() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/indexset/process_create_node.wt.idx".to_string(),
        )
        .await;

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/indexset/process_create_node.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = BTreeMap::<String, Link, UnsizedNode<Pair<String, Link>>>::new();
        let (_, cdc) = indexset.insert_cdc(
            "Something from someone".to_string(),
            Link {
                page_id: 0.into(),
                offset: 0,
                length: 24,
            },
        );
        for event in cdc {
            space_index.process_change_event(event).await.unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/indexset/process_create_node.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/indexset/process_create_node.wt.idx"
                .to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/indexset/process_insert_at.wt.idx".to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/indexset/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/indexset/process_insert_at.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = space_index.parse_indexset().await.unwrap();
        let (_, cdc) = indexset.insert_cdc(
            "Someone from somewhere".to_string(),
            Link {
                page_id: 0.into(),
                offset: 24,
                length: 48,
            },
        );
        for event in cdc {
            space_index.process_change_event(event).await.unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/indexset/process_insert_at.wt.idx".to_string(),
            "tests/data/expected/space_index_unsized/indexset/process_insert_at.wt.idx".to_string()
        ))
    }

    #[tokio::test]
    async fn test_space_index_process_insert_at_big_amount() {
        remove_file_if_exists(
            "tests/data/space_index_unsized/indexset/process_insert_at_big_amount.wt.idx"
                .to_string(),
        )
        .await;
        copy(
            "tests/data/expected/space_index_unsized/process_create_node.wt.idx",
            "tests/data/space_index_unsized/indexset/process_insert_at_big_amount.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndexUnsized::<String, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index_unsized/indexset/process_insert_at_big_amount.wt.idx",
            0.into(),
        )
        .await
        .unwrap();
        let indexset = space_index.parse_indexset().await.unwrap();

        for i in 0..512 {
            let (_, cdc) = indexset.insert_cdc(
                format!("Value number {}", i),
                Link {
                    page_id: 0.into(),
                    offset: i * 24,
                    length: 24,
                },
            );
            for event in cdc {
                space_index.process_change_event(event).await.unwrap();
            }
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index_unsized/indexset/process_insert_at_big_amount.wt.idx"
                .to_string(),
            "tests/data/expected/space_index_unsized/indexset/process_insert_at_big_amount.wt.idx"
                .to_string()
        ))
    }
}
