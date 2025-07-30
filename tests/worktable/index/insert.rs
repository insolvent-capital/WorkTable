use std::sync::Arc;
use std::thread;
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
        attr4: String,
    },
    indexes: {
        attr1_idx: attr1,
        attr2_idx: attr2 unique,
        attr4_idx: attr4 unique,
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
        attr4: "Attribute4".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select(2).is_none())
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
        attr4: "Attribute4".to_string(),
    };
    let pk = table.insert(row.clone()).unwrap();

    let next_row = TestRow {
        id: pk.0,
        val: 0,
        attr1: "some str".to_string(),
        attr2: 0,
        attr3: 0,
        attr4: "Attributee".to_string(),
    };
    assert!(table.insert(next_row.clone()).is_err());
    assert_eq!(table.select(pk.clone()).unwrap(), row);
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
        attr4: "Attribute4".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    let next_row = TestRow {
        id: table.get_next_pk().into(),
        val: 0,
        attr1: "some str".to_string(),
        attr2: row.attr2,
        attr3: 0,
        attr4: "Attributeee".to_string(),
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
    assert_eq!(
        table
            .0
            .indexes
            .attr2_idx
            .get(&row.attr2)
            .map(|r| r.get().value),
        table
            .0
            .pk_map
            .get(&TestPrimaryKey(row.id))
            .map(|r| r.get().value)
    );
}

#[test]
fn insert_when_secondary_unique_string_exists() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: table.get_next_pk().into(),
        val: 13,
        attr1: "Attribute".to_string(),
        attr2: -128,
        attr3: 123456789,
        attr4: "Attribute4".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    let next_row = TestRow {
        id: table.get_next_pk().into(),
        val: 0,
        attr1: "some str".to_string(),
        attr2: 128,
        attr3: 0,
        attr4: "Attribute4".to_string(),
    };
    assert!(table.insert(next_row.clone()).is_err());
    assert!(table
        .0
        .indexes
        .attr1_idx
        .get(&next_row.attr1)
        .collect::<Vec<_>>()
        .is_empty());
    assert!(table.0.indexes.attr4_idx.get(&row.attr4).is_some());
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
    assert_eq!(
        table
            .0
            .indexes
            .attr4_idx
            .get(&row.attr4)
            .map(|r| r.get().value),
        table
            .0
            .pk_map
            .get(&TestPrimaryKey(row.id))
            .map(|r| r.get().value)
    );
}

#[test]
fn insert_when_unique_violated() {
    let table = Arc::new(TestWorkTable::default());

    let row = TestRow {
        id: table.get_next_pk().into(),
        val: 13,
        attr1: "Attribute".to_string(),
        attr2: -128,
        attr3: 123456789,
        attr4: "Attribute4".to_string(),
    };
    let _ = table.insert(row.clone()).unwrap();

    let row_new_attr_2 = 128;
    let row_new_attr_4 = row.attr4.clone();

    let shared = table.clone();
    let h = thread::spawn(move || {
        for _ in 0..5_000 {
            let row = TestRow {
                id: shared.get_next_pk().into(),
                val: 13,
                attr1: "Attribute".to_string(),
                attr2: row_new_attr_2,
                attr3: 123456789,
                attr4: row_new_attr_4.clone(),
            };
            assert!(shared.insert(row).is_err());
        }
    });

    for _ in 0..5000 {
        let attr_1_rows = table.select_by_attr1(row.attr1.clone()).execute().unwrap();
        assert_eq!(attr_1_rows.len(), 1);
        assert_eq!(attr_1_rows.first().unwrap(), &row);
        let row_new_attr_2_row = table.select_by_attr2(row_new_attr_2);
        assert!(row_new_attr_2_row.is_none());
    }

    h.join().unwrap();
}
