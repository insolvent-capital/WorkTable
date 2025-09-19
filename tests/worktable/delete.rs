use worktable::prelude::*;
use worktable::worktable;

worktable!(
    name: DeleteTest,
    columns: {
        token: String primary_key,
        val1: u64,
        val2: u64,
    },
    indexes: {
        val1_idx: val1,
        val2_idx: val2,
    },
    queries: {
        update: {
            Val1ByToken(val1) by token,
        },
        delete: {
            ByVal1() by val1,
        }
    }
);
