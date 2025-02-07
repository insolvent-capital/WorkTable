use async_std::task;
use std::collections::HashMap;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    // describe WorkTable
    worktable!(
        name: My,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            attr: String,
            attr2: i16,

        },
        indexes: {
            idx1: attr,
            idx2: attr2,
        },
        queries: {
            update: {
                ValByAttr(val) by attr,
             //   AttrById(attr) by id,
             //   Attr2ById(attr2) by id,
               AllAttrById(attr, attr2) by id,
            },
            delete: {
                ByAttr() by attr,
                ById() by id,
            }
        }
    );

    // Init Worktable
    let my_table = MyWorkTable::default();

    // WT rows (has prefix My because of table name)
    let row = MyRow {
        val: 777,
        attr: "TEST".to_string(),
        attr2: 345,
        id: 0,
    };

    let row1 = MyRow {
        val: 2,
        attr: "TEST2".to_string(),
        attr2: 123,
        id: 1,
    };

    let row2 = MyRow {
        val: 1337,
        attr: "TEST2".to_string(),
        attr2: 345,
        id: 2,
    };

    let row3 = MyRow {
        val: 555,
        attr: "TEST3".to_string(),
        attr2: 123,
        id: 3,
    };

    // insert
    let _ = my_table.insert(row);
    let _ = my_table.insert(row1);
    let _ = my_table.insert(row2);
    let _ = my_table.insert(row3);

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select All records with attribute TEST2
    //let select_by_attr = my_table.select_by_attr("TEST2".to_string());
    //println!(
    //    "Select by Attribute TEST2: {:?}",
    //    select_by_attr.unwrap().vals
    //);

    //let update_val = my_table.update_all_attr_by_id(
    //    AllAttrByIdQuery {
    //        attr: "TEST5".to_string(),
    //        attr2: 1337,
    //    },
    //    MyPrimaryKey(3),
    //);
    //let _ = task::block_on(update_val);

    // Update all recrods val by attr TEST2
    // let update_val = my_table.update_val_by_attr(ValByAttrQuery { val: 777 }, "TEST2".to_string());
    // let _ = task::block_on(update_val);
    //
    // let select_updated = my_table.select_by_attr("TEST2".to_string());
    // println!(
    //     "Select updated by Attribute TEST2: {:?}",
    //     select_updated.unwrap().vals
    // );

    // Update attr by ID
    //println!("update attr TEST3 -> TEST2");
    //  let update_attr = my_table.update_attr_by_id(
    //            attr: "TEST2".to_string(),
    //      },
    //    MyPrimaryKey(3),
    //);
    // let _ = task::block_on(update_attr);

    // println!("FINISH update attr TEST3 -> TEST2");

    // Update attr2 by ID
    // println!("update attr2 67 -> 1337");
    // let update_attr = my_table.update_attr_2_by_id(Attr2ByIdQuery { attr2: 1337 }, MyPrimaryKey(3));
    // let _ = task::block_on(update_attr);

    //println!("FINISH update attr2");

    // Update record attribute TEST2 -> TEST3 with id 1
    //let update_exchange =
    //    my_table.update_val_by_attr(ValByAttrQuery { val: 7777 }, "TEST2".to_string());
    //let _ = task::block_on(update_exchange);
    //
    // let select_all_after_update = my_table.select_all();
    // println!(
    //     "Select After Val Update by Attribute: {:?}",
    //     select_all_after_update.execute()
    // );
    let test_delete = my_table.delete_by_attr("TEST2".to_string());
    let _ = task::block_on(test_delete);
    // //
    // let select_by_attr = my_table.select_by_attr("TEST2".to_string());
    // println!(
    //     "Select by Attribute TEST2 after del: {:?}",
    //     select_by_attr.unwrap().vals
    // );
    // //
    // let select_by_attr = my_table.select_by_attr("TEST3".to_string());
    // println!(
    //     "Select by Attribute TEST3 after del: {:?}",
    //     select_by_attr.unwrap().vals
    // );

    let all_update = my_table.update_all_attr_by_id(
        AllAttrByIdQuery {
            attr: "test".to_string(),
            attr2: 1337,
        },
        MyPrimaryKey(0),
    );
    let _ = task::block_on(all_update);

    //    let select_by_attr = my_table.select_by_attr("test".to_string());
    //    println!(
    //        "Select by Attribute 222  after del: {:?}",
    //        select_by_attr.unwrap().vals
    //    );
    //
    println!("Select ALL {:?}", my_table.select_all().execute());

    let select_by_attr = my_table.select_by_attr("test".to_string());
    println!("Select by Attribute OK: {:?}", select_by_attr.unwrap().vals);

    let select_by_attr2 = my_table.select_by_attr2(1337);
    println!(
        "Select by Attribute 1337: {:?}",
        select_by_attr2.unwrap().vals
    );
}
