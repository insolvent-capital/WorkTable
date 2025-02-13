use worktable::prelude::*;
use worktable::worktable;

// The test checks updates for 3 indecies at once
worktable!(
    name: Test3,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
        attr3: u64,
    },
    indexes: {
        idx1: attr1,
        idx2: attr2,
        idx3: attr3,
    },
    queries: {
        update: {
            ThreeAttrById(attr1, attr2, attr3) by id,
        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn update_3_idx() {
    let test_table = Test3WorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3Row {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        attr3: attr3_old,
        id: 0,
    };

    let attr1_new = "1337".to_string();
    let attr2_new = 1337;
    let attr3_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update_three_attr_by_id(
            ThreeAttrByIdQuery {
                attr1: attr1_new.clone(),
                attr2: attr2_new,
                attr3: attr3_new,
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table.select_by_attr1(attr1_new.clone()).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr1, attr1_new);

    let updated = test_table.select_by_attr2(attr2_new).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr2, attr2_new);

    let updated = test_table.select_by_attr3(attr3_new).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr3, attr3_new);

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone()).unwrap();
    assert_eq!(updated.vals.first(), None);

    let updated = test_table.select_by_attr2(attr2_old).unwrap();
    assert_eq!(updated.vals.first(), None);

    let updated = test_table.select_by_attr3(attr3_old).unwrap();
    assert_eq!(updated.vals.first(), None);
}

// The test checks updates for 2 indecies at once

worktable!(
    name: Test2,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
    },
    indexes: {
        idx1: attr1,
        idx2: attr2,
    },
    queries: {
        update: {
            AllAttrById(attr1, attr2) by id,
        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn update_2_idx() {
    let test_table = Test2WorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = Test2Row {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();
    let attr2_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update_all_attr_by_id(
            AllAttrByIdQuery {
                attr1: attr1_new.clone(),
                attr2: attr2_new,
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table.select_by_attr1(attr1_new.clone()).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr1, attr1_new);

    let updated = test_table.select_by_attr2(attr2_new).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr2, attr2_new);

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone()).unwrap();
    assert_eq!(updated.vals.first(), None);

    let updated = test_table.select_by_attr2(attr2_old).unwrap();
    assert_eq!(updated.vals.first(), None);
}

// The test checks updates for 1 index

worktable!(
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
    },
    indexes: {
        idx1: attr1,
    },
    queries: {
        update: {
            ValByAttr(val) by attr1,
            Attr1ById(attr1) by id,

        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn update_1_idx() {
    let test_table = TestWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;

    let row = TestRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        id: 0,
    };

    let attr1_new = "OK".to_string();

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update_attr_1_by_id(
            Attr1ByIdQuery {
                attr1: attr1_new.clone(),
            },
            pk.clone(),
        )
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table.select_by_attr1(attr1_new.clone()).unwrap();
    assert_eq!(updated.vals.first().unwrap().attr1, attr1_new);

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone()).unwrap();
    assert_eq!(updated.vals.first(), None);
}
