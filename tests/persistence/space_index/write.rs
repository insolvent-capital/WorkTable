use std::fs::copy;

use data_bucket::{Link, INNER_PAGE_SIZE};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable::prelude::{SpaceIndex, SpaceIndexOps};

use crate::{check_if_files_are_same, remove_file_if_exists};

mod run_first {
    use super::*;

    #[test]
    fn test_space_index_process_create_node() {
        remove_file_if_exists("tests/data/space_index/process_create_node.wt.idx".to_string());

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_create_node.wt.idx",
            0.into(),
        )
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                max_value: Pair {
                    key: 5,
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_create_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
        ))
    }

    #[test]
    fn test_space_index_process_create_second_node() {
        remove_file_if_exists(
            "tests/data/space_index/process_create_second_node.wt.idx".to_string(),
        );
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_create_second_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_create_second_node.wt.idx",
            0.into(),
        )
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::CreateNode {
                max_value: Pair {
                    key: 15,
                    value: Link {
                        page_id: 1.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_create_second_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_create_second_node.wt.idx".to_string()
        ))
    }

    #[test]
    fn test_space_index_process_insert_at() {
        remove_file_if_exists("tests/data/space_index/process_insert_at.wt.idx".to_string());
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_insert_at.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_insert_at.wt.idx",
            0.into(),
        )
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                max_value: Pair {
                    key: 5,
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
                value: Pair {
                    key: 3,
                    value: Link {
                        page_id: 0.into(),
                        offset: 24,
                        length: 48,
                    },
                },
                index: 0,
            })
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_insert_at.wt.idx".to_string(),
            "tests/data/expected/space_index/process_insert_at.wt.idx".to_string()
        ))
    }

    #[test]
    fn test_space_index_process_insert_at_big_amount() {
        remove_file_if_exists(
            "tests/data/space_index/process_insert_at_big_amount.wt.idx".to_string(),
        );
        copy(
            "tests/data/expected/space_index/process_create_node.wt.idx",
            "tests/data/space_index/process_insert_at_big_amount.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_insert_at_big_amount.wt.idx",
            0.into(),
        )
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::InsertAt {
                max_value: Pair {
                    key: 5,
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
                value: Pair {
                    key: 1000,
                    value: Link {
                        page_id: 0.into(),
                        offset: 24,
                        length: 24,
                    },
                },
                index: 1,
            })
            .unwrap();

        for i in (6..911).rev() {
            space_index
                .process_change_event(ChangeEvent::InsertAt {
                    max_value: Pair {
                        key: 1000,
                        value: Link {
                            page_id: 0.into(),
                            offset: 24,
                            length: 24,
                        },
                    },
                    value: Pair {
                        key: i,
                        value: Link {
                            page_id: 0.into(),
                            offset: i * 24,
                            length: 24,
                        },
                    },
                    index: 1,
                })
                .unwrap();
        }

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_insert_at_big_amount.wt.idx".to_string(),
            "tests/data/expected/space_index/process_insert_at_big_amount.wt.idx".to_string()
        ))
    }

    #[test]
    fn test_space_index_process_remove_node() {
        remove_file_if_exists("tests/data/space_index/process_remove_node.wt.idx".to_string());
        copy(
            "tests/data/expected/space_index/process_create_second_node.wt.idx",
            "tests/data/space_index/process_remove_node.wt.idx",
        )
        .unwrap();

        let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
            "tests/data/space_index/process_remove_node.wt.idx",
            0.into(),
        )
        .unwrap();

        space_index
            .process_change_event(ChangeEvent::RemoveNode {
                max_value: Pair {
                    key: 5,
                    value: Link {
                        page_id: 0.into(),
                        offset: 0,
                        length: 24,
                    },
                },
            })
            .unwrap();

        assert!(check_if_files_are_same(
            "tests/data/space_index/process_remove_node.wt.idx".to_string(),
            "tests/data/expected/space_index/process_remove_node.wt.idx".to_string()
        ))
    }
}

#[test]
fn test_space_index_process_insert_at_with_node_id_update() {
    remove_file_if_exists(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string(),
    );
    copy(
        "tests/data/expected/space_index/process_create_node.wt.idx",
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 1,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_insert_at_with_node_id_update.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at_with_node_id_update.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_remove_at() {
    remove_file_if_exists("tests/data/space_index/process_remove_at.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_remove_at.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_remove_at.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 3,
                value: Link {
                    page_id: 0.into(),
                    offset: 24,
                    length: 48,
                },
            },
            index: 0,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_remove_at.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_remove_at_node_id() {
    remove_file_if_exists("tests/data/space_index/process_remove_at_node_id.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_remove_at_node_id.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_remove_at_node_id.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_remove_at_node_id.wt.idx".to_string(),
        "tests/data/expected/space_index/process_remove_at_node_id.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_insert_at_removed_place() {
    remove_file_if_exists(
        "tests/data/space_index/process_insert_at_removed_place.wt.idx".to_string(),
    );
    copy(
        "tests/data/expected/space_index/process_insert_at.wt.idx",
        "tests/data/space_index/process_insert_at_removed_place.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_insert_at_removed_place.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::InsertAt {
            max_value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            value: Pair {
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            index: 2,
        })
        .unwrap();
    space_index
        .process_change_event(ChangeEvent::RemoveAt {
            max_value: Pair {
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: 5,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .unwrap();
    space_index
        .process_change_event(ChangeEvent::InsertAt {
            max_value: Pair {
                key: 7,
                value: Link {
                    page_id: 0.into(),
                    offset: 72,
                    length: 24,
                },
            },
            value: Pair {
                key: 6,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            index: 1,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_insert_at_removed_place.wt.idx".to_string(),
        "tests/data/expected/space_index/process_insert_at_removed_place.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_create_node_after_remove() {
    remove_file_if_exists(
        "tests/data/space_index/process_create_node_after_remove.wt.idx".to_string(),
    );
    copy(
        "tests/data/expected/space_index/process_remove_node.wt.idx",
        "tests/data/space_index/process_create_node_after_remove.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_create_node_after_remove.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::CreateNode {
            max_value: Pair {
                key: 10,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_create_node_after_remove.wt.idx".to_string(),
        "tests/data/expected/space_index/process_create_node_after_remove.wt.idx".to_string()
    ))
}

#[test]
fn test_space_index_process_split_node() {
    remove_file_if_exists("tests/data/space_index/process_split_node.wt.idx".to_string());
    copy(
        "tests/data/expected/space_index/process_insert_at_big_amount.wt.idx",
        "tests/data/space_index/process_split_node.wt.idx",
    )
    .unwrap();

    let mut space_index = SpaceIndex::<u32, { INNER_PAGE_SIZE as u32 }>::new(
        "tests/data/space_index/process_split_node.wt.idx",
        0.into(),
    )
    .unwrap();

    space_index
        .process_change_event(ChangeEvent::SplitNode {
            max_value: Pair {
                key: 1000,
                value: Link {
                    page_id: 0.into(),
                    offset: 0,
                    length: 24,
                },
            },
            split_index: 453,
        })
        .unwrap();

    assert!(check_if_files_are_same(
        "tests/data/space_index/process_split_node.wt.idx".to_string(),
        "tests/data/expected/space_index/process_split_node.wt.idx".to_string()
    ))
}
