use uuid::Uuid;
use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: Uuid primary_key,
        another: i64,
    }
);

#[test]
fn insert() {
    let table = TestWorkTable::default();
    let row = TestRow {
        id: Uuid::new_v4(),
        another: 1,
    };
    let pk = table.insert(row.clone()).unwrap();
    let selected_row = table.select(pk).unwrap();

    assert_eq!(selected_row, row);
    assert!(table.select(Uuid::new_v4()).is_none())
}
