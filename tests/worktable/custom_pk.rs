use std::sync::atomic::{AtomicU64, Ordering};

use rkyv::{Archive, Deserialize, Serialize};
use worktable::prelude::*;
use worktable::worktable;

#[derive(
    Archive,
    Debug,
    Default,
    Deserialize,
    Clone,
    Eq,
    From,
    PartialOrd,
    PartialEq,
    Ord,
    Serialize,
    SizeMeasure,
)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(Debug))]
struct CustomId(u64);

#[derive(Debug, Default)]
pub struct Generator(AtomicU64);

impl PrimaryKeyGenerator<TestPrimaryKey> for Generator {
    fn next(&self) -> TestPrimaryKey {
        let res = self.0.fetch_add(1, Ordering::Relaxed);

        if res >= 10 {
            self.0.store(0, Ordering::Relaxed);
        }

        CustomId::from(res).into()
    }
}

impl TablePrimaryKey for TestPrimaryKey {
    type Generator = Generator;
}

worktable! (
    name: Test,
    columns: {
        id: CustomId primary_key custom,
        test: u64
    }
);

#[test]
fn test_custom_pk() {
    let table = TestWorkTable::default();
    let pk = table.get_next_pk();
    assert_eq!(pk, CustomId::from(0).into());

    for _ in 0..10 {
        let _ = table.get_next_pk();
    }
    let pk = table.get_next_pk();
    assert_eq!(pk, CustomId::from(0).into());
}