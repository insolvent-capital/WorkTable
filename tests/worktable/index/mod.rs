mod insert;
mod update_by_pk;
mod update_full;
mod update_query;

use worktable::prelude::*;
use worktable::worktable;

// The test checks updates for 3 indecies at once
worktable!(
    name: Test3Unique,
    columns: {
        id: u64 primary_key autoincrement,
        val: i64,
        attr1: String,
        attr2: i16,
        attr3: u64,
    },
    indexes: {
        idx1: attr1 unique,
        idx2: attr2 unique,
        idx3: attr3 unique,
    },
    queries: {
        update: {
            UniqueThreeAttrById(attr1, attr2, attr3) by id,
            UniqueTwoAttrByThird(attr1, attr2) by attr3,
        },
        delete: {
            ById() by id,
        }
    }
);

// The test checks updates for 3 indecies at once
worktable!(
    name: Test3NonUnique,
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
            TwoAttrByThird(attr1, attr2) by attr3,
        },
        delete: {
            ById() by id,
        }
    }
);

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
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr2, attr2_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
    let updated = test_table
        .select_by_attr2(attr2_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

#[tokio::test]
async fn update_2_idx_full_row() {
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
        .update(Test2Row {
            id: pk.clone().into(),
            attr1: attr1_new.clone(),
            attr2: attr2_new,
            val: row.val,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr2, attr2_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
    let updated = test_table
        .select_by_attr2(attr2_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
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
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

#[tokio::test]
async fn update_1_idx_full_row() {
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
        .update(TestRow {
            attr2: row.attr2,
            id: pk.clone().into(),
            attr1: attr1_new.clone(),
            val: row.val,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr1, attr1_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}
