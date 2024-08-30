use worktable_codegen::worktable;

#[test]
fn test() {
    worktable! (
            name: Test,
            columns: {
                id: u64 primary_key,
                test: i64,
                exchnage: String
            }
        );

    println!("{size}")
}