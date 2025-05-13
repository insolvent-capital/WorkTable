use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
        attr3: u64,
    },
    indexes: {
        attr1_idx: attr1,
        attr2_idx: attr2 unique,
    }
);

#[test]
fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        val: 13,
        attr1: "Attribute".to_string(),
        attr2: -128,
        attr3: 123456789,
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select(2.into()).is_none())
}

#[test]
fn insert_when_pk_exists() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        val: 13,
        attr1: "Attribute".to_string(),
        attr2: -128,
        attr3: 123456789,
    };
    let pk = table.insert(row.clone()).unwrap();

    let next_row = TestRow {
        id: pk.0,
        val: 0,
        attr1: "some str".to_string(),
        attr2: 0,
        attr3: 0,
    };
    assert!(table.insert(next_row.clone()).is_err());
    assert!(table
        .0
        .indexes
        .attr1_idx
        .get(&next_row.attr1)
        .collect::<Vec<_>>()
        .is_empty());
    assert!(table.0.indexes.attr2_idx.get(&next_row.attr2).is_none());
    assert_eq!(
        table
            .0
            .indexes
            .attr1_idx
            .get(&row.attr1)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert!(table.0.indexes.attr2_idx.get(&row.attr2).is_some())
}

#[test]
fn insert_when_secondary_unique_exists() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        val: 13,
        attr1: "Attribute".to_string(),
        attr2: -128,
        attr3: 123456789,
    };
    let _ = table.insert(row.clone()).unwrap();

    let next_row = TestRow {
        id: table.get_next_pk().into(),
        val: 0,
        attr1: "some str".to_string(),
        attr2: row.attr2,
        attr3: 0,
    };
    assert!(table.insert(next_row.clone()).is_err());
    assert!(table
        .0
        .indexes
        .attr1_idx
        .get(&next_row.attr1)
        .collect::<Vec<_>>()
        .is_empty());
    assert_eq!(
        table
            .0
            .indexes
            .attr1_idx
            .get(&row.attr1)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert!(table.0.indexes.attr2_idx.get(&row.attr2).is_some());
}
