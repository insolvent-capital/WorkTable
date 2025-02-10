use async_std::task;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    // describe WorkTable
    worktable!(
        name: My,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            test: u8 optional,
            attr: String,
            attr2: i16,

        },
        indexes: {
            idx1: attr,
            idx2: attr2,
        },
        queries: {
            update: {
                ValById(val) by id,
             //   AttrById(attr) by id,
             //   Attr2ById(attr2) by id,
                  AllAttrById(attr, attr2) by id,
                  UpdateOptionalById(test) by id,
            },
            delete: {
              //  ByAttr() by attr,
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
        test: Some(1),
        id: 0,
    };

    //let row1 = MyRow {
    //    val: 2,
    //    attr: "TEST2".to_string(),
    //    attr2: 123,
    //    id: 1,
    //};
    //
    //let row2 = MyRow {
    //    val: 1337,
    //    attr: "TEST2".to_string(),
    //    attr2: 345,
    //    id: 2,
    //};
    //
    //let row3 = MyRow {
    //    val: 555,
    //    attr: "TEST3".to_string(),
    //    attr2: 123,
    //    id: 3,
    //};

    // insert
    let _ = my_table.insert(row);
    // let _ = my_table.insert(row1);
    // let _ = my_table.insert(row2);
    // let _ = my_table.insert(row3);

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select All records with attribute TEST
    //  let select_by_attr = my_table.select_by_attr("TEST".to_string());
    //  println!(
    //      "Select by Attribute TEST: {:?}",
    //      select_by_attr.unwrap().vals
    //  );
    //
    //  let update_val = my_table.update_all_attr_by_id(
    //      AllAttrByIdQuery {
    //          attr: "TEST5".to_string(),
    //          attr2: 1337,
    //      },
    //      MyPrimaryKey(0),
    //  );
    //  let _ = task::block_on(update_val);
    //
    //  let select_by_attr2 = my_table.select_by_attr2(1337);
    //   println!(
    //       "Select by Attribute 1337: {:?}",
    //       select_by_attr2.unwrap().vals
    //   );
}
