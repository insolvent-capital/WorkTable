use worktable_codegen::worktable;

#[test]
fn test() {
    worktable! (
        name: Test,
        columns: {
            id: u64 primary_key,
            test: i64
        }
    );

    let row = TestRow {
        test: 1,
        id: 1
    };

    println!("{:?}", row);
}
