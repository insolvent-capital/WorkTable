use futures::executor::block_on;
use worktable::prelude::*;
use worktable::worktable;

fn main() {
    // describe WorkTable
    worktable!(
        name: My,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            test: u8,
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
                AllAttrById(attr, attr2) by id,
                UpdateOptionalById(test) by id,
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
        attr: "Attribute1".to_string(),
        attr2: 345,
        test: 1,
        id: 0,
    };

    // insert
    let pk: MyPrimaryKey = my_table.insert(row).expect("primary key");

    // Select ALL records from WT
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select All records with attribute TEST
    let select_all = my_table.select_all().execute();
    println!("Select All {:?}", select_all);

    // Select by Idx
    let select_by_attr = my_table.select_by_attr("Attribute1".to_string());
    println!("Select by idx {:?}", select_by_attr.unwrap().vals);

    // Update Value query
    let update = my_table.update_val_by_id(ValByIdQuery { val: 1337 }, pk.clone());
    let _ = block_on(update);

    let select_all = my_table.select_all().execute();
    println!("Select after update val {:?}", select_all);

    let delete = my_table.delete(pk);
    let _ = block_on(delete);

    let select_all = my_table.select_all().execute();
    println!("Select after delete {:?}", select_all);
}
