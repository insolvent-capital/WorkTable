use crate::worktable::index::{
    Test3NonUniqueRow, Test3NonUniqueWorkTable, Test3UniqueRow, Test3UniqueWorkTable,
};
use worktable::prelude::SelectQueryExecutor;

#[tokio::test]
async fn update_by_full_row_unique_indexes() {
    let test_table = Test3UniqueWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3UniqueRow {
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
        .update(Test3UniqueRow {
            attr1: attr1_new.clone(),
            id: pk.clone().into(),
            val: row.val,
            attr2: attr2_new,
            attr3: attr3_new,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table.select_by_attr1(attr1_new.clone());
    assert_eq!(updated.unwrap().attr1, attr1_new);
    let updated = test_table.select_by_attr2(attr2_new);
    assert_eq!(updated.unwrap().attr2, attr2_new);
    let updated = test_table.select_by_attr3(attr3_new);
    assert_eq!(updated.unwrap().attr3, attr3_new);

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone());
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr2(attr2_old);
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr3(attr3_old);
    assert_eq!(updated, None);
}

#[tokio::test]
async fn update_by_full_row_non_unique_indexes() {
    let test_table = Test3NonUniqueWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3NonUniqueRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        attr3: attr3_old,
        id: 0,
    };

    let attr1_new = "1337".to_string();
    let attr2_new = 1000;
    let attr3_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update(Test3NonUniqueRow {
            attr1: attr1_new.clone(),
            id: pk.clone().into(),
            val: row.val,
            attr2: attr2_new,
            attr3: attr3_new,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx1
            .get(&attr1_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx2
            .get(&attr2_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr2, attr2_new);
    let updated = test_table
        .select_by_attr3(attr3_new)
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx3
            .get(&attr3_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr3, attr3_new);

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
    let updated = test_table
        .select_by_attr3(attr3_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}

#[tokio::test]
async fn update_by_full_row_unique_with_string_update() {
    let test_table = Test3UniqueWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3UniqueRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        attr3: attr3_old,
        id: 0,
    };

    let attr1_new = "TEST 1337 TO BE BIGGER THAN PREVIOUS".to_string();
    let attr2_new = 1337;
    let attr3_new = 1337;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update(Test3UniqueRow {
            attr1: attr1_new.clone(),
            id: pk.clone().into(),
            val: row.val,
            attr2: attr2_new,
            attr3: attr3_new,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table.select_by_attr1(attr1_new.clone());
    assert_eq!(updated.unwrap().attr1, attr1_new);
    let updated = test_table.select_by_attr2(attr2_new);
    assert_eq!(updated.unwrap().attr2, attr2_new);
    let updated = test_table.select_by_attr3(attr3_new);
    assert_eq!(updated.unwrap().attr3, attr3_new);

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone());
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr2(attr2_old);
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr3(attr3_old);
    assert_eq!(updated, None);
}

#[tokio::test]
async fn update_by_full_row_non_unique_with_string_update() {
    let test_table = Test3NonUniqueWorkTable::default();

    let attr1_old = "TEST".to_string();
    let attr2_old = 1000;
    let attr3_old = 65000;

    let row = Test3NonUniqueRow {
        val: 1,
        attr1: attr1_old.clone(),
        attr2: attr2_old,
        attr3: attr3_old,
        id: 0,
    };

    let attr1_new = "TEST 1337 TO BE BIGGER THAN PREVIOUS".to_string();
    let attr2_new = 1337;
    let attr3_new = 65000;

    let pk = test_table.insert(row.clone()).unwrap();
    test_table
        .update(Test3NonUniqueRow {
            attr1: attr1_new.clone(),
            id: pk.clone().into(),
            val: row.val,
            attr2: attr2_new,
            attr3: attr3_new,
        })
        .await
        .unwrap();

    // Checks idx updated
    let updated = test_table
        .select_by_attr1(attr1_new.clone())
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx1
            .get(&attr1_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx2
            .get(&attr2_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr2, attr2_new);
    let updated = test_table
        .select_by_attr3(attr3_new)
        .execute()
        .expect("rows");
    assert_eq!(
        test_table
            .0
            .indexes
            .idx3
            .get(&attr3_new)
            .collect::<Vec<_>>()
            .len(),
        1
    );
    assert_eq!(updated.first().unwrap().attr3, attr3_new);

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
async fn update_by_full_row_with_reinsert_and_primary_key_violation() {
    let test_table = Test3UniqueWorkTable::default();

    let row1 = Test3UniqueRow {
        val: 1,
        attr1: "TEST".to_string(),
        attr2: 1000,
        attr3: 65000,
        id: 0,
    };
    test_table.insert(row1.clone()).unwrap();
    let row2 = Test3UniqueRow {
        val: 1,
        attr1: "TEST1".to_string(),
        attr2: 1001,
        attr3: 65001,
        id: 1,
    };
    test_table.insert(row2.clone()).unwrap();
    let mut update = row1.clone();
    update.id = row2.id.clone();
    update.attr1 = "TEST_______________________1".to_string();
    assert!(test_table.update(update).await.is_err());

    assert_eq!(test_table.select(row1.id).unwrap(), row1);
    assert_eq!(
        test_table.select_by_attr1(row1.attr1.clone()).unwrap(),
        row1
    );
    assert_eq!(test_table.select_by_attr2(row1.attr2).unwrap(), row1);
    assert_eq!(test_table.select_by_attr3(row1.attr3).unwrap(), row1);

    assert_eq!(test_table.select(row2.id).unwrap(), row2);
    assert_eq!(
        test_table.select_by_attr1(row2.attr1.clone()).unwrap(),
        row2
    );
    assert_eq!(test_table.select_by_attr2(row2.attr2).unwrap(), row2);
    assert_eq!(test_table.select_by_attr3(row2.attr3).unwrap(), row2);
}

#[tokio::test]
async fn update_by_full_row_with_reinsert_and_secondary_unique_violation() {
    let test_table = Test3UniqueWorkTable::default();

    let row1 = Test3UniqueRow {
        val: 1,
        attr1: "TEST".to_string(),
        attr2: 1000,
        attr3: 65000,
        id: 0,
    };
    test_table.insert(row1.clone()).unwrap();
    let row2 = Test3UniqueRow {
        val: 1,
        attr1: "TEST__________________1".to_string(),
        attr2: 1001,
        attr3: 65001,
        id: 1,
    };
    test_table.insert(row2.clone()).unwrap();
    let mut update = row1.clone();
    update.attr1 = row2.attr1.clone();
    assert!(test_table.update(update).await.is_err());

    assert_eq!(test_table.select(row1.id).unwrap(), row1);
    assert_eq!(
        test_table.select_by_attr1(row1.attr1.clone()).unwrap(),
        row1
    );
    assert_eq!(test_table.select_by_attr2(row1.attr2).unwrap(), row1);
    assert_eq!(test_table.select_by_attr3(row1.attr3).unwrap(), row1);

    assert_eq!(test_table.select(row2.id).unwrap(), row2);
    assert_eq!(
        test_table.select_by_attr1(row2.attr1.clone()).unwrap(),
        row2
    );
    assert_eq!(test_table.select_by_attr2(row2.attr2).unwrap(), row2);
    assert_eq!(test_table.select_by_attr3(row2.attr3).unwrap(), row2);
}

#[tokio::test]
async fn update_by_full_row_with_secondary_unique_violation() {
    let test_table = Test3UniqueWorkTable::default();

    let row1 = Test3UniqueRow {
        val: 1,
        attr1: "TEST".to_string(),
        attr2: 1000,
        attr3: 65000,
        id: 0,
    };
    test_table.insert(row1.clone()).unwrap();
    let row2 = Test3UniqueRow {
        val: 1,
        attr1: "TEST1".to_string(),
        attr2: 1001,
        attr3: 65001,
        id: 1,
    };
    test_table.insert(row2.clone()).unwrap();
    let mut update = row1.clone();
    update.attr2 = row2.attr2.clone();
    assert!(test_table.update(update).await.is_err());

    assert_eq!(test_table.select(row1.id).unwrap(), row1);
    assert_eq!(
        test_table.select_by_attr1(row1.attr1.clone()).unwrap(),
        row1
    );
    assert_eq!(test_table.select_by_attr2(row1.attr2).unwrap(), row1);
    assert_eq!(test_table.select_by_attr3(row1.attr3).unwrap(), row1);

    assert_eq!(test_table.select(row2.id).unwrap(), row2);
    assert_eq!(
        test_table.select_by_attr1(row2.attr1.clone()).unwrap(),
        row2
    );
    assert_eq!(test_table.select_by_attr2(row2.attr2).unwrap(), row2);
    assert_eq!(test_table.select_by_attr3(row2.attr3).unwrap(), row2);
}
