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
    let attr2_new = 1337;
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
    assert_eq!(updated.first().unwrap().attr1, attr1_new);
    let updated = test_table
        .select_by_attr2(attr2_new)
        .execute()
        .expect("rows");
    assert_eq!(updated.first().unwrap().attr2, attr2_new);
    let updated = test_table
        .select_by_attr3(attr3_new)
        .execute()
        .expect("rows");
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
    let updated = test_table
        .select_by_attr3(attr3_old)
        .execute()
        .expect("rows");
    assert_eq!(updated.first(), None);
}
