use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: TestFloat,
    columns: {
        id: u64 primary_key autoincrement,
        test: i64,
        another: f64,
        exchange: String
    },
    indexes: {
        test_idx: test unique,
        exchnage_idx: exchange,
        another_idx: another
    }
);

#[test]
fn select_all_range_float_test() {
    let table = TestFloatWorkTable::default();

    let row1 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 100.0.into(),
        exchange: "M".to_string(),
    };
    let row2 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 200.0.into(),
        exchange: "N".to_string(),
    };
    let row3 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 300.0.into(),
        exchange: "P".to_string(),
    };

    let _ = table.insert(row1.clone()).unwrap();
    let _ = table.insert(row2.clone()).unwrap();
    let _ = table.insert(row3.clone()).unwrap();

    let all = table
        .select_all()
        .where_by(|row| row.another > 99.0.into() && row.another < 300.0.into())
        .execute()
        .unwrap();

    assert_eq!(all.len(), 2);
    assert!(all.contains(&row1));
    assert!(all.contains(&row2))
}

#[test]
fn select_by_another_test() {
    let table = TestFloatWorkTable::default();

    let row1 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 3,
        another: 100.0.into(),
        exchange: "M".to_string(),
    };
    let row2 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 1,
        another: 100.0.into(),
        exchange: "N".to_string(),
    };
    let row3 = TestFloatRow {
        id: table.get_next_pk().into(),
        test: 2,
        another: 200.0.into(),
        exchange: "P".to_string(),
    };

    let _ = table.insert(row1.clone()).unwrap();
    let _ = table.insert(row2.clone()).unwrap();
    let _ = table.insert(row3.clone()).unwrap();

    let where_100 = table.select_by_another(100.0.into()).execute().unwrap();
    assert_eq!(where_100.len(), 2);
    assert!(where_100.contains(&row1));
    assert!(where_100.contains(&row2));
    let where_200 = table.select_by_another(200.0.into()).execute().unwrap();
    assert_eq!(where_200.len(), 1);
    assert!(where_200.contains(&row3));
}
