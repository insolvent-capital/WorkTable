use crate::worktable::index::{
    Test3NonUniqueRow, Test3NonUniqueWorkTable, Test3UniqueRow, Test3UniqueWorkTable,
    TwoAttrByThirdQuery, UniqueTwoAttrByThirdQuery,
};
use worktable::prelude::SelectQueryExecutor;

#[tokio::test]
async fn update_two_via_query_unique_indexes() {
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

    let _ = test_table.insert(row.clone()).unwrap();
    test_table
        .update_unique_two_attr_by_third(
            UniqueTwoAttrByThirdQuery {
                attr1: attr1_new.clone(),
                attr2: attr2_new,
            },
            attr3_old,
        )
        .await
        .unwrap();

    let mut new_row = row.clone();
    new_row.attr1 = attr1_new;
    new_row.attr2 = attr2_new;

    // Check old idx removed
    let updated = test_table.select_by_attr1(attr1_old.clone());
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr2(attr2_old);
    assert_eq!(updated, None);
    let updated = test_table.select_by_attr3(attr3_old);
    assert!(updated.is_some());
    assert_eq!(updated, Some(new_row))
}

#[tokio::test]
async fn update_with_reinsert_and_secondary_unique_violation() {
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
    let update = UniqueTwoAttrByThirdQuery {
        attr1: row2.attr1.clone(),
        attr2: 999,
    };
    assert!(
        test_table
            .update_unique_two_attr_by_third(update, row1.attr3,)
            .await
            .is_err()
    );

    assert_eq!(
        test_table.select_by_attr1(row1.attr1.clone()).unwrap(),
        row1
    );
    assert_eq!(test_table.select_by_attr2(row1.attr2).unwrap(), row1);
    assert_eq!(test_table.select_by_attr3(row1.attr3).unwrap(), row1);

    assert_eq!(
        test_table.select_by_attr1(row2.attr1.clone()).unwrap(),
        row2
    );
    assert_eq!(test_table.select_by_attr2(row2.attr2).unwrap(), row2);
    assert_eq!(test_table.select_by_attr3(row2.attr3).unwrap(), row2);
}

#[tokio::test]
async fn update_with_secondary_unique_violation() {
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
    let update = UniqueTwoAttrByThirdQuery {
        attr1: row1.attr1.clone(),
        attr2: row2.attr2,
    };
    assert!(
        test_table
            .update_unique_two_attr_by_third(update, row1.attr3)
            .await
            .is_err()
    );

    assert_eq!(
        test_table.select_by_attr1(row1.attr1.clone()).unwrap(),
        row1
    );
    assert_eq!(test_table.select_by_attr2(row1.attr2).unwrap(), row1);
    assert_eq!(test_table.select_by_attr3(row1.attr3).unwrap(), row1);

    assert_eq!(
        test_table.select_by_attr1(row2.attr1.clone()).unwrap(),
        row2
    );
    assert_eq!(test_table.select_by_attr2(row2.attr2).unwrap(), row2);
    assert_eq!(test_table.select_by_attr3(row2.attr3).unwrap(), row2);
}

#[tokio::test]
async fn update_two_via_query_non_unique_indexes() {
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

    let _ = test_table.insert(row.clone()).unwrap();
    test_table
        .update_two_attr_by_third(
            TwoAttrByThirdQuery {
                attr1: attr1_new,
                attr2: attr2_new,
            },
            attr3_old,
        )
        .await
        .unwrap();

    // Check old idx removed
    let updated = test_table
        .select_by_attr1(attr1_old.clone())
        .execute()
        .unwrap();
    assert!(updated.is_empty());
    let updated = test_table.select_by_attr2(attr2_old).execute().unwrap();
    assert!(updated.is_empty());
    let updated = test_table.select_by_attr3(attr3_old).execute().unwrap();
    assert!(!updated.is_empty());
}
