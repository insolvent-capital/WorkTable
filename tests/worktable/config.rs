use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
        something: String,
        another: u32,
    },
    config: {
        row_derives: Default,
        page_size: 32_000,
    }
);

#[test]
fn test_page_size() {
    assert_eq!(TEST_PAGE_SIZE, 32_000)
}

#[test]
fn test_default_available() {
    let d = TestRow::default();
    assert_eq!(d.id, u64::default());
    assert_eq!(d.something, String::default());
    assert_eq!(d.another, u32::default())
}
