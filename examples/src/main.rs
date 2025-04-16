use futures::executor::block_on;
use worktable::prelude::*;
use worktable::worktable;

#[tokio::main]
async fn main() {
    // describe WorkTable
    worktable!(
        name: My,
        persist: true,
        columns: {
            id: u64 primary_key autoincrement,
            val: i64,
            test: i32,
            attr: String,
            attr2: i32,
            attr_float: f64,
            attr_string: String,

        },
        indexes: {
            idx1: attr,
            idx2: attr2 unique,
            idx3: attr_string,
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
    let config = PersistenceConfig::new("data", "data");
    let my_table = MyWorkTable::new(config).await.unwrap();

    // WT rows (has prefix My because of table name)
    let row = MyRow {
        val: 777,
        attr: "Attribute0".to_string(),
        attr2: 345,
        test: 1,
        id: 0,
        attr_float: 100.0,
        attr_string: "String_attr0".to_string(),
    };

    for i in 2..1000000_i64 {
        let row = MyRow {
            val: 777,
            attr: format!("Attribute{}", i),
            attr2: 345 + i as i32,
            test: i as i32,
            id: i as u64,
            attr_float: 100.0 + i as f64,
            attr_string: format!("String_attr{}", i),
        };

        my_table.insert(row).unwrap();
    }

    // insert
    let pk: MyPrimaryKey = my_table.insert(row).expect("primary key");

    // Select ALL records from WT
    let _select_all = my_table.select_all().execute();
    //println!("Select All {:?}", select_all);

    // Select All records with attribute TEST
    let _select_all = my_table.select_all().execute();
    //println!("Select All {:?}", select_all);

    // Select by Idx
    //let _select_by_attr = my_table
    //   .select_by_attr("Attribute1".to_string())
    //    .execute()
    //r    .unwrap();

    //for row in select_by_attr {
    //    println!("Select by idx, row {:?}", row);
    //}

    // Update Value query
    let update = my_table.update_val_by_id(ValByIdQuery { val: 1337 }, pk.clone());
    let _ = block_on(update);

    let _select_all = my_table.select_all().execute();
    //println!("Select after update val {:?}", select_all);

    let delete = my_table.delete(pk);
    let _ = block_on(delete);

    let _select_all = my_table.select_all().execute();
    //println!("Select after delete {:?}", select_all);

    let info = my_table.system_info();

    println!("{info}");
}
