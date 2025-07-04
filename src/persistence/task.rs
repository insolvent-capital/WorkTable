use data_bucket::page::PageId;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use worktable_codegen::worktable;

use crate::persistence::operation::{
    BatchInnerRow, BatchInnerWorkTable, BatchOperation, OperationId, PosByOpIdQuery,
};
use crate::persistence::PersistenceEngineOps;
use crate::prelude::*;
use crate::util::OptimizedVec;

worktable! (
    name: QueueInner,
    columns: {
        id: u64 primary_key autoincrement,
        operation_id: OperationId,
        page_id: PageId,
        link: Link,
        pos: usize,
    },
    indexes: {
        operation_id_idx: operation_id,
        page_id_idx: page_id,
        link_idx: link,
    },
);

const MAX_PAGE_AMOUNT: usize = 16;

pub struct QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    operations: OptimizedVec<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    queue_inner_wt: Arc<QueueInnerWorkTable>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    QueueAnalyzer<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
where
    PrimaryKeyGenState: Debug,
    PrimaryKey: Debug,
    SecondaryKeys: Debug,
{
    pub fn new(queue_inner_wt: Arc<QueueInnerWorkTable>) -> Self {
        Self {
            operations: OptimizedVec::with_capacity(256),
            queue_inner_wt,
        }
    }

    pub fn push(
        &mut self,
        value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>,
    ) -> eyre::Result<()> {
        let link = value.link();
        let mut row = QueueInnerRow {
            id: self.queue_inner_wt.get_next_pk().into(),
            operation_id: value.operation_id(),
            page_id: link.page_id,
            link,
            pos: 0,
        };
        let pos = self.operations.push(value);
        row.pos = pos;
        self.queue_inner_wt.insert(row)?;
        Ok(())
    }

    pub fn extend_from_iter(
        &mut self,
        i: impl Iterator<Item = Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    ) -> eyre::Result<()> {
        for op in i {
            self.push(op)?
        }
        Ok(())
    }

    pub fn get_first_op_id_available(&self) -> Option<OperationId> {
        self.queue_inner_wt
            .0
            .indexes
            .operation_id_idx
            .iter()
            .next()
            .map(|(id, _)| *id)
    }

    pub async fn collect_batch_from_op_id(
        &mut self,
        op_id: OperationId,
    ) -> eyre::Result<BatchOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>
    where
        PrimaryKeyGenState: Clone,
        PrimaryKey: Clone,
        SecondaryKeys: Clone,
    {
        let mut ops_set = HashSet::new();
        let mut used_page_ids = HashSet::new();

        let mut next_op_id = op_id;
        let mut no_more_ops = false;
        while used_page_ids.len() < MAX_PAGE_AMOUNT && !no_more_ops {
            let ops_rows = self
                .queue_inner_wt
                .select_by_operation_id(next_op_id)
                .execute()?;
            match next_op_id {
                OperationId::Single(_) => {
                    let page_id = ops_rows
                        .first()
                        .expect("at least one row should be available as operation exists")
                        .page_id;
                    used_page_ids.insert(page_id);
                    let page_ops = self.queue_inner_wt.select_by_page_id(page_id).execute()?;
                    let max_op_id = &mut next_op_id;
                    ops_set.extend(page_ops.into_iter().map(move |r| {
                        if r.operation_id > *max_op_id {
                            *max_op_id = r.operation_id
                        }
                        r.operation_id
                    }));
                }
                OperationId::Multi(_) => {
                    let mut ops_set_to_extend = HashSet::new();
                    used_page_ids.extend(ops_rows.iter().map(|r| r.page_id));
                    for page_id in ops_rows.iter().map(|r| r.page_id) {
                        let page_ops = self.queue_inner_wt.select_by_page_id(page_id).execute()?;
                        ops_set_to_extend.extend(page_ops.into_iter().map(|r| r.operation_id));
                    }
                    let mut block_op_id = None;
                    for op_id in ops_set_to_extend.iter().filter(|op_id| match op_id {
                        OperationId::Single(_) => false,
                        OperationId::Multi(_) => true,
                    }) {
                        let rows = self
                            .queue_inner_wt
                            .select_by_operation_id(*op_id)
                            .execute()?;
                        let pages = rows.iter().map(|r| r.page_id).collect::<HashSet<_>>();
                        // if pages used by multi op are not available is used_page_ids set, it's blocker op
                        for page in pages.iter() {
                            if !used_page_ids.contains(page) {
                                if let Some(block_op_id) = block_op_id.as_mut() {
                                    if *block_op_id > *op_id {
                                        *block_op_id = *op_id
                                    }
                                } else {
                                    block_op_id = Some(*op_id)
                                }
                            }
                        }
                    }
                    // And if we found some blocker, we need to remove all ops after blocking op.
                    let ops_set_to_extend = if let Some(block_op_id) = block_op_id {
                        ops_set_to_extend
                            .into_iter()
                            .filter(|op_id| *op_id >= block_op_id)
                            .collect()
                    } else {
                        ops_set_to_extend
                    };
                    ops_set.extend(ops_set_to_extend);
                    no_more_ops = true;
                }
            };
            let mut range = self
                .queue_inner_wt
                .0
                .indexes
                .operation_id_idx
                .range(next_op_id..);
            if let Some((id, _)) = range.nth(1) {
                next_op_id = *id;
            } else {
                no_more_ops = true
            }
        }
        // After this point, we have ops set ready for batch generation.
        let mut ops_pos_set = HashSet::new();
        for op_id in ops_set {
            let rows = self
                .queue_inner_wt
                .select_by_operation_id(op_id)
                .execute()?;
            ops_pos_set.extend(rows.into_iter().map(|r| (r.pos, r.id)))
        }

        let mut ops = Vec::with_capacity(ops_pos_set.len());
        let info_wt = BatchInnerWorkTable::default();
        for (pos, id) in ops_pos_set {
            let mut row: BatchInnerRow = self
                .queue_inner_wt
                .select(id.into())
                .expect("exists as Id exists")
                .into();
            let op = self
                .operations
                .remove(pos)
                .expect("should be available as presented in table");
            row.pos = ops.len();
            row.op_type = op.operation_type();
            ops.push(op);
            info_wt.insert(row)?;
            self.queue_inner_wt.delete_without_lock(id.into())?
        }
        // println!("New wt generated {:?}", start.elapsed());
        // return ops sorted by `OperationId`
        ops.sort_by_key(|k| k.operation_id());
        for (pos, op) in ops.iter().enumerate() {
            let op_id = op.operation_id();
            let q = PosByOpIdQuery { pos };
            info_wt.update_pos_by_op_id(q, op_id).await?;
        }

        Ok(BatchOperation { ops, info_wt })
    }

    pub fn len(&self) -> usize {
        self.queue_inner_wt.count()
    }
}

#[derive(Debug)]
pub struct Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    queue: lockfree::queue::Queue<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    notify: Notify,
    len: Arc<AtomicU16>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn new() -> Self {
        Self {
            queue: lockfree::queue::Queue::new(),
            notify: Notify::new(),
            len: Arc::new(AtomicU16::new(0)),
        }
    }

    pub fn push(&self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.len.fetch_add(1, Ordering::Release);
        self.queue.push(value);
        self.notify.notify_one();
    }

    pub async fn pop(&self) -> Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
        loop {
            // Drain values
            if let Some(value) = self.queue.pop() {
                self.len.fetch_sub(1, Ordering::Release);
                return value;
            }

            // Wait for values to be available
            self.notify.notified().await;
        }
    }

    pub fn immediate_pop(
        &self,
    ) -> Option<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        if let Some(v) = self.queue.pop() {
            self.len.fetch_sub(1, Ordering::Release);
            Some(v)
        } else {
            None
        }
    }

    pub fn pop_iter(
        &self,
    ) -> impl Iterator<Item = Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        let iter_count = self.len.clone();
        self.queue.pop_iter().inspect(move |_| {
            iter_count.fetch_sub(1, Ordering::Release);
        })
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire) as usize
    }
}

#[derive(Debug)]
pub struct PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    #[allow(dead_code)]
    engine_task_handle: tokio::task::AbortHandle,
    queue: Arc<Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    analyzer_inner_wt: Arc<QueueInnerWorkTable>,
    analyzer_in_progress: Arc<AtomicBool>,
    progress_notify: Arc<Notify>,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn apply_operation(&self, op: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.queue.push(op);
    }

    pub fn run_engine<E>(mut engine: E) -> Self
    where
        E: PersistenceEngineOps<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> + Send + 'static,
        SecondaryKeys: Clone + Debug + Send + Sync + 'static,
        PrimaryKeyGenState: Clone + Debug + Send + Sync + 'static,
        PrimaryKey: Clone + Debug + Send + Sync + 'static,
    {
        let queue = Arc::new(Queue::new());
        let progress_notify = Arc::new(Notify::new());

        let engine_queue = queue.clone();
        let engine_progress_notify = progress_notify.clone();
        let analyzer_inner_wt: Arc<QueueInnerWorkTable> = Default::default();
        let mut analyzer = QueueAnalyzer::new(analyzer_inner_wt.clone());
        let analyzer_in_progress = Arc::new(AtomicBool::new(true));
        let task_analyzer_in_progress = analyzer_in_progress.clone();

        let task = async move {
            loop {
                let op = if let Some(next_op) = engine_queue.immediate_pop() {
                    Some(next_op)
                } else if analyzer.len() == 0 {
                    engine_progress_notify.notify_waiters();
                    task_analyzer_in_progress.store(false, Ordering::Release);
                    let res = Some(engine_queue.pop().await);
                    task_analyzer_in_progress.store(true, Ordering::Release);
                    res
                } else {
                    None
                };
                if let Some(op) = op {
                    if let Err(err) = analyzer.push(op) {
                        tracing::warn!("Error while feeding data to analyzer: {}", err);
                    }
                }
                let ops_available_iter = engine_queue.pop_iter();
                if let Err(err) = analyzer.extend_from_iter(ops_available_iter) {
                    tracing::warn!("Error while feeding data to analyzer: {}", err);
                }
                if let Some(op_id) = analyzer.get_first_op_id_available() {
                    let batch_op = analyzer.collect_batch_from_op_id(op_id).await;
                    if let Err(e) = batch_op {
                        tracing::warn!("Error collecting batch operation: {}", e);
                    } else {
                        let batch_op = batch_op.unwrap();
                        let res = engine.apply_batch_operation(batch_op).await;
                        if let Err(e) = res {
                            tracing::warn!(
                                "Persistence engine failed while applying batch op: {}",
                                e
                            );
                        }
                    }
                }
            }
        };
        let engine_task_handle = tokio::spawn(task).abort_handle();
        Self {
            queue,
            engine_task_handle,
            analyzer_inner_wt,
            analyzer_in_progress,
            progress_notify,
        }
    }

    fn check_wait_triggers(&self) -> bool {
        if self.queue.len() != 0 {
            return false;
        }
        if self.analyzer_inner_wt.count() != 0 {
            return false;
        }
        if self.analyzer_in_progress.load(Ordering::Acquire) {
            return false;
        }
        true
    }

    pub async fn wait_for_ops(&self) {
        while !self.check_wait_triggers() {
            let queue_count = self.queue.len();
            let analyzer_count = self.analyzer_inner_wt.count();
            let count = queue_count + analyzer_count;
            if count == 0 {
                tracing::info!("Waiting for last operation");
            } else {
                tracing::info!("Waiting for {} operations", count);
            }

            tokio::select! {
                _ = self.progress_notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}
