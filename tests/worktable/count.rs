use worktable::prelude::*;
use worktable::worktable;

// The test checks updates for 3 indecies at once
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
        idx2: attr2 unique,
    },
    queries: {
        update: {
            ThreeAttrById(attr1, attr2) by id,
        },
        delete: {
            ById() by id,
        }
    }
);

#[tokio::test]
async fn count() {
    let test_table = TestWorkTable::default();

    let attr = "Attr1".to_string();

    let row1 = TestRow {
        val: 1,
        attr1: attr.clone(),
        attr2: 1,
        id: 1,
    };

    let row2 = TestRow {
        val: 1,
        attr1: attr.clone(),
        attr2: 2,
        id: 2,
    };

    let row3 = TestRow {
        val: 1,
        attr1: attr.clone(),
        attr2: 3,
        id: 3,
    };

    let row4 = TestRow {
        val: 1,
        attr1: attr.clone(),
        attr2: 4,
        id: 4,
    };

    // Count WT with 0 rows
    assert_eq!(0, test_table.count());

    let _ = test_table.insert(row1);
    let _ = test_table.insert(row2);
    let _ = test_table.insert(row3);
    let _ = test_table.insert(row4);

    // Count by WT
    assert_eq!(4, test_table.count());
}
