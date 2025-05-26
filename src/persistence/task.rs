use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::persistence::PersistenceEngineOps;
use crate::prelude::Operation;

#[derive(Debug)]
pub struct Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    queue: lockfree::queue::Queue<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    notify: Notify,
    len: AtomicU16,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn new() -> Self {
        Self {
            queue: lockfree::queue::Queue::new(),
            notify: Notify::new(),
            len: AtomicU16::new(0),
        }
    }

    pub fn push(&self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.queue.push(value);
        self.len.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    pub async fn pop(&self) -> Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
        loop {
            // Drain values
            if let Some(value) = self.queue.pop() {
                self.len.fetch_sub(1, Ordering::Relaxed);
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
            self.len.fetch_sub(1, Ordering::Relaxed);
            Some(v)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) as usize
    }
}

#[derive(Debug)]
pub struct PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    #[allow(dead_code)]
    engine_task_handle: tokio::task::AbortHandle,
    queue: Arc<Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    progress_notify: Arc<Notify>,
    // True if non-empty, false either.
    wait_state: Arc<AtomicBool>,
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
        SecondaryKeys: Debug + Send + 'static,
        PrimaryKeyGenState: Debug + Send + 'static,
        PrimaryKey: Debug + Send + 'static,
    {
        let queue = Arc::new(Queue::new());
        let progress_notify = Arc::new(Notify::new());
        let wait_state = Arc::new(AtomicBool::new(false));

        let engine_queue = queue.clone();
        let engine_progress_notify = progress_notify.clone();
        let engine_wait_state = wait_state.clone();
        let task = async move {
            loop {
                let next_op = if let Some(next_op) = engine_queue.immediate_pop() {
                    next_op
                } else {
                    engine_wait_state.store(true, Ordering::Relaxed);
                    engine_progress_notify.notify_waiters();
                    let res = engine_queue.pop().await;
                    engine_wait_state.store(false, Ordering::Relaxed);
                    res
                };
                tracing::debug!("Applying operation {:?}", next_op);
                let res = engine.apply_operation(next_op).await;
                if let Err(err) = res {
                    tracing::warn!("{}", err);
                }
            }
        };
        let engine_task_handle = tokio::spawn(task).abort_handle();
        Self {
            queue,
            engine_task_handle,
            progress_notify,
            wait_state,
        }
    }

    pub async fn wait_for_ops(&self) {
        if !self.wait_state.load(Ordering::Relaxed) {
            let count = self.queue.len();
            println!("Waiting for {} operations", count);
            self.progress_notify.notified().await
        }
    }
}
