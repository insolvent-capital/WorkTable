use crate::worktable::index::WorkTableError::NotFound;
use std::collections::HashMap;
use worktable::prelude::*;
use worktable::worktable;

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
        idx2: attr2,
    },
    queries: {
        update: {
            ValByAttr(val) by attr1,
            AttrById(attr1) by id,
            Attr2ById(attr2) by id,
            AllAttrById(attr1, attr2) by id,
        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn update_index() {
    let test_table = TestWorkTable::default();

    let row = TestRow {
        val: 1,
        attr1: "TEST".to_string(),
        attr2: 1000,
        id: 0,
    };
    let pk = test_table.insert(row.clone()).unwrap();
    let _all_update = test_table.update_all_attr_by_id(
        AllAttrByIdQuery {
            attr1: "TEST2".to_string(),
            attr2: 1337,
        },
        pk.clone(),
    );

    let updated = test_table.select(pk).unwrap();
    assert_eq!(updated, row);

    let binding = test_table.select_by_attr2(1337).unwrap();
    let found = binding.vals.first().unwrap();
    assert_eq!(found, &updated);

    //let found = test_table
    //    .select_by_attr1("TEST2".to_string())
    //    .unwrap()
    //    .vals
    //    .first()
    //    .unwrap();
    //assert_eq!(found, &updated);
    //
    //let not_found = test_table
    //    .select_by_attr2(1000)
    //    .unwrap()
    //    .vals
    //    .first()
    //    .unwrap();
    //assert_eq!(not_found, &updated);

    // let not_found = test_table.select_by_attr2(1000);
    // assert_eq!(not_found, NotFound);
}
