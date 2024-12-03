use worktable::prelude::*;
use worktable::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement,
    },
    config: {
        page_size: 32_000,
    }
);

#[test]
fn test_page_size() {
    assert_eq!(TEST_PAGE_SIZE, 32_000)
}
