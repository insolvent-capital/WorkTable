use worktable_codegen::worktable;
use worktable::prelude::*;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement TreeIndex,
        test: u64,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique,
        exchnage_idx: exchange,
    },
    queries: {
        update: {
            TestById(test) by id,
            TestByAnother(test) by another,
            TestByExchange(test) by exchange,
        }
    }
);