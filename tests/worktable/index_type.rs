use worktable::prelude::*;
use worktable_codegen::worktable;

worktable! (
    name: Test,
    columns: {
        id: u64 primary_key autoincrement IndexSet,
        test: u64,
        another: u64,
        exchange: i32,
    },
    indexes: {
        another_idx: another unique IndexSet,
        exchnage_idx: exchange IndexSet,
    },
    queries: {
        update: {
            TestById(test) by id,
            TestByAnother(test) by another,
            TestByExchange(test) by exchange,
        }
    }
);
