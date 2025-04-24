use rand::distr::{Alphanumeric, SampleString};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

use worktable::prelude::*;
use worktable_codegen::worktable;

worktable!(
    name: Map,
    columns: {
        id: u64 primary_key,
        value: String
    },
    queries: {
        update: {
            ValueById(value) by id,
        }
    }
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rw_lock_hash_map_vs_wt() {
    let wt = Arc::new(MapWorkTable::default());
    let hash_map = Arc::new(RwLock::new(HashMap::<u64, String>::default()));

    println!("Inserting...");
    let map_start = Instant::now();
    for i in 0..100u64 {
        let mut map = hash_map.write().await;
        let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
        map.insert(i, s);
    }
    println!("map insert in {} μs", map_start.elapsed().as_micros());

    let wt_start = Instant::now();
    for i in 0..100 {
        let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
        let row = MapRow { id: i, value: s };
        wt.insert(row).unwrap();
    }
    println!("wt insert in {} μs", wt_start.elapsed().as_micros());

    println!("Updating...");
    let map_start = Instant::now();
    let task_map = hash_map.clone();
    let h = tokio::task::spawn(async move {
        for i in 0..100000u64 {
            let mut map = task_map.write().await;
            let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
            map.insert((i % 50) * 2, s);
        }
    });
    for i in 0..100000u64 {
        let mut map = hash_map.write().await;
        let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
        map.insert((i % 50) * 2 + 1, s);
    }
    h.await.unwrap();
    println!("map update in {} μs", map_start.elapsed().as_micros());

    let wt_start = Instant::now();
    let task_wt = wt.clone();
    let h = tokio::task::spawn(async move {
        for i in 0..100000u64 {
            let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
            let q = ValueByIdQuery { value: s };
            task_wt
                .update_value_by_id(q, ((i % 50) * 2).into())
                .await
                .unwrap();
        }
    });
    for i in 0..100000u64 {
        let s: String = Alphanumeric.sample_string(&mut rand::rng(), 8);
        let q = ValueByIdQuery { value: s };
        wt.update_value_by_id(q, ((i % 50) * 2 + 1).into())
            .await
            .unwrap();
    }
    h.await.unwrap();
    println!("wt update in {} μs", wt_start.elapsed().as_micros());
}
