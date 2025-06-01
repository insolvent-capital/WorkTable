use data_bucket::{parse_page, Link, UnsizedIndexPage, INNER_PAGE_SIZE};
use tokio::fs::OpenOptions;

#[tokio::test]
async fn test_index_page_read_after_create_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Something from someone".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Something from someone".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    )
}

#[tokio::test]
async fn test_index_pages_read_after_creation_of_second_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_create_second_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Something from someone".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Something from someone".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    );

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 3)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Someone from somewhere".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Someone from somewhere".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 1.into(),
            offset: 24,
            length: 32,
        }
    )
}

#[tokio::test]
async fn test_index_pages_read_after_remove_node_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_remove_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 3)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Someone from somewhere".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Someone from somewhere".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 1.into(),
            offset: 24,
            length: 32,
        }
    )
}

#[tokio::test]
async fn test_index_pages_read_after_insert_at_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_insert_at.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Something from someone".to_string());
    assert_eq!(page.inner.index_values.len(), 2);
    let first_value = &page.inner.index_values[0];
    assert_eq!(first_value.key, "Something else".to_string());
    assert_eq!(
        first_value.link,
        Link {
            page_id: 0.into(),
            offset: 24,
            length: 48,
        }
    );
    let second_value = &page.inner.index_values[1];
    assert_eq!(second_value.key, "Something from someone".to_string());
    assert_eq!(
        second_value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    );
}

#[tokio::test]
async fn test_index_page_read_after_remove_at_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_remove_at.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Something from someone".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Something from someone".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    )
}

#[tokio::test]
async fn test_index_page_read_after_remove_at_node_id_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_remove_at_node_id.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(page.inner.node_id.key, "Something else".to_string());
    assert_eq!(page.inner.index_values.len(), 1);
    let value = page.inner.index_values.first().unwrap();
    assert_eq!(value.key, "Something else".to_string());
    assert_eq!(
        value.link,
        Link {
            page_id: 0.into(),
            offset: 24,
            length: 48,
        }
    )
}

#[tokio::test]
async fn test_index_page_read_after_insert_at_with_node_id_update_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(
            "tests/data/expected/space_index_unsized/process_insert_at_with_node_id_update.wt.idx",
        )
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(
        page.inner.node_id.key,
        "Something from someone 1".to_string()
    );
    assert_eq!(page.inner.index_values.len(), 2);
    let first_value = &page.inner.index_values[0];
    assert_eq!(first_value.key, "Something from someone".to_string());
    assert_eq!(
        first_value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    );
    let second_value = &page.inner.index_values[1];
    assert_eq!(second_value.key, "Something from someone 1".to_string());
    assert_eq!(
        second_value.link,
        Link {
            page_id: 0.into(),
            offset: 24,
            length: 48,
        }
    )
}

#[tokio::test]
async fn test_index_page_read_after_insert_at_removed_place_in_space_index() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_insert_at_removed_place.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();

    assert_eq!(
        page.inner.node_id.key,
        "Something from someone 1".to_string()
    );
    assert_eq!(page.inner.index_values.len(), 3);
    let first_value = &page.inner.index_values[0];
    assert_eq!(first_value.key, "Something else".to_string());
    assert_eq!(
        first_value.link,
        Link {
            page_id: 0.into(),
            offset: 24,
            length: 48,
        }
    );
    let second_value = &page.inner.index_values[1];
    assert_eq!(second_value.key, "Something from someone 0".to_string());
    assert_eq!(
        second_value.link,
        Link {
            page_id: 0.into(),
            offset: 0,
            length: 24,
        }
    );
    let third_value = &page.inner.index_values[2];
    assert_eq!(third_value.key, "Something from someone 1".to_string());
    assert_eq!(
        third_value.link,
        Link {
            page_id: 0.into(),
            offset: 72,
            length: 24,
        }
    )
}

#[tokio::test]
async fn test_index_pages_read_after_node_split() {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open("tests/data/expected/space_index_unsized/process_split_node.wt.idx")
        .await
        .unwrap();

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 2)
    .await
    .unwrap();
    assert_eq!(page.inner.node_id.key, "Something from someone 52");
    assert_eq!(page.inner.slots_size, 53);

    let page = parse_page::<
        UnsizedIndexPage<String, { INNER_PAGE_SIZE as u32 }>,
        { INNER_PAGE_SIZE as u32 },
    >(&mut file, 3)
    .await
    .unwrap();
    assert_eq!(page.inner.node_id.key, "Something from someone _100");
    assert_eq!(page.inner.slots_size, 48);
}
