use std::collections::HashMap;
use std::fmt::Debug;

use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::QueueInnerRow;
use crate::prelude::*;
use crate::prelude::{From, Order, SelectQueryExecutor};
use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use derive_more::Display;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;
use worktable_codegen::{worktable, MemStat};

/// Represents page's identifier. Is unique within the table bounds
#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[rkyv(derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
pub enum OperationId {
    #[from]
    Single(Uuid),
    Multi(Uuid),
}

impl SizeMeasurable for OperationId {
    fn aligned_size(&self) -> usize {
        Uuid::default().aligned_size()
    }
}

impl Default for OperationId {
    fn default() -> Self {
        OperationId::Single(Uuid::now_v7())
    }
}

#[derive(
    Archive,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Serialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[repr(u8)]
pub enum OperationType {
    #[default]
    Insert,
    Update,
    Delete,
}

impl SizeMeasurable for OperationType {
    fn aligned_size(&self) -> usize {
        u8::default().aligned_size()
    }
}

worktable! (
    name: BatchInner,
    columns: {
        id: u64 primary_key autoincrement,
        operation_id: OperationId,
        page_id: PageId,
        link: Link,
        op_type: OperationType,
        pos: usize,
    },
    indexes: {
        operation_id_idx: operation_id,
        page_id_idx: page_id,
        link_idx: link,
        op_type_idx: op_type,
    },
    queries: {
        update: {
            PosByOpId(pos) by operation_id,
        },
    }
);

impl BatchInnerWorkTable {
    pub fn iter_links(&self) -> impl Iterator<Item = Link> {
        self.0
            .indexes
            .link_idx
            .iter()
            .map(|(l, _)| *l)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl From<QueueInnerRow> for BatchInnerRow {
    fn from(value: QueueInnerRow) -> Self {
        BatchInnerRow {
            id: value.id,
            operation_id: value.operation_id,
            page_id: value.page_id,
            link: value.link,
            op_type: Default::default(),
            pos: 0,
        }
    }
}

#[derive(Debug)]
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    pub ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    pub info_wt: BatchInnerWorkTable,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryKeys: Debug + Default + Clone + TableSecondaryIndexEventsOps,
{
    pub fn get_pk_gen_state(&self) -> eyre::Result<Option<PrimaryKeyGenState>> {
        let row = self
            .info_wt
            .select_by_op_type(OperationType::Insert)
            .order_on(BatchInnerRowFields::OperationId, Order::Desc)
            .limit(1)
            .execute()?;
        Ok(row.into_iter().next().map(|r| {
            let pos = r.pos;
            let op = self.ops.get(pos).expect("available as pos in wt");
            op.pk_gen_state().expect("is insert operation").clone()
        }))
    }

    pub fn get_indexes_evs(&self) -> eyre::Result<(BatchChangeEvent<PrimaryKey>, SecondaryKeys)> {
        let mut primary = vec![];
        let mut secondary = SecondaryKeys::default();

        let mut rows = self.info_wt.select_all().execute()?;
        rows.sort_by(|l, r| l.operation_id.cmp(&r.operation_id));
        for row in rows {
            let pos = row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(evs) = op.primary_key_events() {
                primary.extend(evs.iter().cloned())
            }
            let secondary_new = op.secondary_key_events();
            secondary.extend(secondary_new.clone());
        }

        Ok((primary, secondary))
    }

    pub fn get_batch_data_op(&self) -> eyre::Result<BatchData> {
        let mut data = HashMap::new();
        for link in self.info_wt.iter_links() {
            let last_op = self
                .info_wt
                .select_by_link(link)
                .order_on(BatchInnerRowFields::OperationId, Order::Desc)
                .limit(1)
                .execute()?;
            let op_row = last_op
                .into_iter()
                .next()
                .expect("if link is in info_wt at least one row exists");
            let pos = op_row.pos;
            let op = self
                .ops
                .get(pos)
                .expect("pos should be correct as was set while batch build");
            if let Some(data_bytes) = op.bytes() {
                let link = op.link();
                data.entry(link.page_id)
                    .and_modify(|v: &mut Vec<_>| v.push((link, data_bytes.to_vec())))
                    .or_insert(vec![(link, data_bytes.to_vec())]);
            }
        }

        Ok(data)
    }
}

#[derive(Clone, Debug)]
pub enum Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    Insert(InsertOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>),
    Update(UpdateOperation<SecondaryKeys>),
    Delete(DeleteOperation<PrimaryKey, SecondaryKeys>),
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn operation_type(&self) -> OperationType {
        match &self {
            Operation::Insert(_) => OperationType::Insert,
            Operation::Update(_) => OperationType::Update,
            Operation::Delete(_) => OperationType::Delete,
        }
    }

    pub fn operation_id(&self) -> OperationId {
        match &self {
            Operation::Insert(insert) => insert.id,
            Operation::Update(update) => update.id,
            Operation::Delete(delete) => delete.id,
        }
    }

    pub fn link(&self) -> Link {
        match &self {
            Operation::Insert(insert) => insert.link,
            Operation::Update(update) => update.link,
            Operation::Delete(delete) => delete.link,
        }
    }

    pub fn bytes(&self) -> Option<&[u8]> {
        match &self {
            Operation::Insert(insert) => Some(&insert.bytes),
            Operation::Update(update) => Some(&update.bytes),
            Operation::Delete(_) => None,
        }
    }

    pub fn primary_key_events(&self) -> Option<&Vec<ChangeEvent<Pair<PrimaryKey, Link>>>> {
        match &self {
            Operation::Insert(insert) => Some(&insert.primary_key_events),
            Operation::Update(_) => None,
            Operation::Delete(delete) => Some(&delete.primary_key_events),
        }
    }

    pub fn secondary_key_events(&self) -> &SecondaryKeys {
        match &self {
            Operation::Insert(insert) => &insert.secondary_keys_events,
            Operation::Update(update) => &update.secondary_keys_events,
            Operation::Delete(delete) => &delete.secondary_keys_events,
        }
    }

    pub fn pk_gen_state(&self) -> Option<&PrimaryKeyGenState> {
        match &self {
            Operation::Insert(insert) => Some(&insert.pk_gen_state),
            Operation::Update(_) => None,
            Operation::Delete(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct InsertOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    pub id: OperationId,
    pub primary_key_events: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    pub secondary_keys_events: SecondaryKeys,
    pub pk_gen_state: PrimaryKeyGenState,
    pub bytes: Vec<u8>,
    pub link: Link,
}

#[derive(Clone, Debug)]
pub struct UpdateOperation<SecondaryKeys> {
    pub id: OperationId,
    pub secondary_keys_events: SecondaryKeys,
    pub bytes: Vec<u8>,
    pub link: Link,
}

#[derive(Clone, Debug)]
pub struct DeleteOperation<PrimaryKey, SecondaryKeys> {
    pub id: OperationId,
    pub primary_key_events: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    pub secondary_keys_events: SecondaryKeys,
    pub link: Link,
}
