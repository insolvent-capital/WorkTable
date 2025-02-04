
use std::collections::HashMap;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    worktable!(
        name: Test,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            attr: String,
        },
        indexes: {
            attr_idx: attr2,
            attr2_idx: attr,
        },
        queries: {
            update: {
                ValByAttr(val) by attr,
               // AttrById(attr) by id,
             //   Attr2ById(attr2) by id,
               AllAttrById(attr, attr2) by id,
            },
            delete: {
                ById() by id,
            }
        }
    );
