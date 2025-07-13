use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use data_bucket::page::PageId;
use data_bucket::{Link, SizeMeasurable};
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;
use worktable_codegen::{worktable, MemStat};

use crate::persistence::space::{BatchChangeEvent, BatchData};
use crate::persistence::task::{LastEventIds, QueueInnerRow};
use crate::persistence::OperationType;
use crate::prelude::*;
use crate::prelude::{From, Order, SelectQueryExecutor};

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
        operation_id_idx: operation_id unique,
        page_id_idx: page_id,
        link_idx: link,
        op_type_idx: op_type,
    },
    queries: {
        update: {
            PosByOpId(pos) by operation_id,
        },
        delete: {
            ByOpId() by operation_id,
        }
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
pub struct BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes> {
    ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
    info_wt: BatchInnerWorkTable,
    prepared_index_evs: Option<PreparedIndexEvents<PrimaryKey, SecondaryEvents>>,
    phantom_data: PhantomData<AvailableIndexes>,
}

#[derive(Debug)]
pub struct PreparedIndexEvents<PrimaryKey, SecondaryEvents> {
    primary_evs: Vec<ChangeEvent<Pair<PrimaryKey, Link>>>,
    secondary_evs: SecondaryEvents,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug,
{
    pub fn new(
        ops: Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>,
        info_wt: BatchInnerWorkTable,
    ) -> Self {
        Self {
            ops,
            info_wt,
            prepared_index_evs: None,
            phantom_data: PhantomData,
        }
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
    BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents, AvailableIndexes>
where
    PrimaryKeyGenState: Debug + Clone,
    PrimaryKey: Debug + Clone,
    SecondaryEvents: Debug + Default + Clone + TableSecondaryIndexEventsOps<AvailableIndexes>,
    AvailableIndexes: Debug + Clone + Copy + Hash + Eq,
{
    pub fn ops(self) -> Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>> {
        self.ops
    }

    fn remove_operations_from_events(
        &mut self,
        invalid_events: PreparedIndexEvents<PrimaryKey, SecondaryEvents>,
    ) -> HashSet<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>> {
        let mut removed_ops = HashSet::new();

        for ev in &invalid_events.primary_evs {
            if let Some(operation_pos_rev) = self.ops.iter().rev().position(|op| {
                if let Some(evs) = op.primary_key_events() {
                    for inner_ev in evs {
                        if inner_ev.id() == ev.id() {
                            return true;
                        }
                    }
                    false
                } else {
                    false
                }
            }) {
                let op = self.ops.remove(self.ops.len() - (operation_pos_rev + 1));
                removed_ops.insert(op);
            }
        }
        for (index, id) in invalid_events.secondary_evs.iter_event_ids() {
            if let Some(operation_pos_rev) = self.ops.iter().rev().position(|op| {
                let evs = op.secondary_key_events();
                evs.contains_event(index, id)
            }) {
                let op = self.ops.remove(self.ops.len() - (operation_pos_rev + 1));
                removed_ops.insert(op);
            };
            // else it was already removed with primary
        }
        for op in &removed_ops {
            let pk = self
                .info_wt
                .select_by_operation_id(op.operation_id())
                .expect("exists as all should be inserted on prepare step")
                .id;
            self.info_wt.delete_without_lock(pk.into()).unwrap();
            let prepared_evs = self
                .prepared_index_evs
                .as_mut()
                .expect("should be set before 0 iteration");
            if let Some(primary_evs) = op.primary_key_events() {
                for ev in primary_evs {
                    if let Ok(pos) = prepared_evs
                        .primary_evs
                        .binary_search_by(|inner_ev| inner_ev.id().cmp(&ev.id()))
                    {
                        prepared_evs.primary_evs.remove(pos);
                    }
                }
            }
            let op_secondary = op.secondary_key_events();
            prepared_evs.secondary_evs.remove(op_secondary);
        }

        removed_ops
    }

    pub fn get_last_event_ids(&self) -> LastEventIds<AvailableIndexes> {
        let prepared_evs = self
            .prepared_index_evs
            .as_ref()
            .expect("should be set before 0 iteration");

        let primary_id = prepared_evs
            .primary_evs
            .last()
            .map(|ev| ev.id())
            .unwrap_or_default();
        let secondary_ids = prepared_evs.secondary_evs.last_evs();
        let secondary_ids = secondary_ids
            .into_iter()
            .map(|(i, v)| (i, v.unwrap_or_default()))
            .collect();
        LastEventIds {
            primary_id,
            secondary_ids,
        }
    }

    pub async fn validate(
        &mut self,
        last_ids: &LastEventIds<AvailableIndexes>,
        attempts: usize,
    ) -> eyre::Result<Option<Vec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryEvents>>>> {
        let mut valid = false;

        self.prepared_index_evs = Some(self.prepare_indexes_evs()?);
        let mut ops_to_remove = vec![];

        while !valid {
            let prepared_evs = self
                .prepared_index_evs
                .as_mut()
                .expect("should be set before 0 iteration");
            let primary_invalid_events = validate_events(&mut prepared_evs.primary_evs);
            let secondary_invalid_events = prepared_evs.secondary_evs.validate();

            valid = if SecondaryEvents::is_unit() {
                primary_invalid_events.is_empty()
            } else {
                primary_invalid_events.is_empty() && secondary_invalid_events.is_empty()
            };

            if valid {
                break;
            }

            let events_to_remove = PreparedIndexEvents {
                primary_evs: primary_invalid_events,
                secondary_evs: secondary_invalid_events,
            };
            let ops = self.remove_operations_from_events(events_to_remove);
            ops_to_remove.extend(ops);
        }

        {
            let prepared_evs = self
                .prepared_index_evs
                .as_ref()
                .expect("should be set before 0 iteration");
            if let Some(id) = prepared_evs.primary_evs.first().map(|ev| ev.id()) {
                if !id.is_next_for(last_ids.primary_id)
                    && last_ids.primary_id != IndexChangeEventId::default()
                {
                    let mut possibly_valid = false;
                    if id.inner().overflowing_sub(last_ids.primary_id.inner()).0 == 2 {
                        // TODO: for split sometimes this happens
                        let ev = prepared_evs.primary_evs.first().unwrap();
                        if let ChangeEvent::SplitNode { .. } = ev {
                            possibly_valid = true
                        }
                        if attempts > 8 {
                            possibly_valid = true
                        }
                    }

                    if !possibly_valid {
                        self.ops.extend(ops_to_remove);
                        return Ok(None);
                    }
                }
            }
            let secondary_first = prepared_evs.secondary_evs.first_evs();
            for (index, id) in secondary_first {
                let Some(last) = last_ids.secondary_ids.get(&index) else {
                    continue;
                };
                if let Some(id) = id {
                    if !id.is_next_for(*last) && *last != IndexChangeEventId::default() {
                        let mut possibly_valid = false;
                        if id.inner().overflowing_sub(last.inner()).0 == 2 {
                            // TODO: for split sometimes this happens
                            possibly_valid = prepared_evs.secondary_evs.is_first_ev_is_split(index);
                            if attempts > 8 {
                                possibly_valid = true
                            }
                        }

                        if !possibly_valid {
                            self.ops.extend(ops_to_remove);
                            return Ok(None);
                        }
                    }
                }
            }
        }

        {
            let prepared_evs = self
                .prepared_index_evs
                .as_ref()
                .expect("should be set before 0 iteration");
            if prepared_evs.primary_evs.is_empty() && prepared_evs.secondary_evs.is_empty() {
                self.ops = ops_to_remove;
                return Ok(None);
            }
        }

        for (pos, op) in self.ops.iter().enumerate() {
            let op_id = op.operation_id();
            let q = PosByOpIdQuery { pos };
            self.info_wt.update_pos_by_op_id(q, op_id).await?
        }

        Ok(Some(ops_to_remove))
    }

    fn prepare_indexes_evs(
        &self,
    ) -> eyre::Result<PreparedIndexEvents<PrimaryKey, SecondaryEvents>> {
        let mut primary_evs = vec![];
        let mut secondary_evs = SecondaryEvents::default();

        for op in &self.ops {
            if let Some(evs) = op.primary_key_events() {
                primary_evs.extend(evs.iter().cloned())
            }
            let secondary_new = op.secondary_key_events();
            secondary_evs.extend(secondary_new.clone());
        }

        // is used to make all events id's monotonically grow
        primary_evs.sort_by_key(|ev1| ev1.id());
        secondary_evs.sort();

        Ok(PreparedIndexEvents {
            primary_evs,
            secondary_evs,
        })
    }

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

    pub fn get_indexes_evs(&self) -> eyre::Result<(BatchChangeEvent<PrimaryKey>, SecondaryEvents)> {
        if let Some(evs) = &self.prepared_index_evs {
            Ok((evs.primary_evs.clone(), evs.secondary_evs.clone()))
        } else {
            tracing::warn!(
                "Index events are not validated and it can cause errors while applying batch"
            );
            let evs = self.prepare_indexes_evs()?;
            Ok((evs.primary_evs.clone(), evs.secondary_evs.clone()))
        }
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
