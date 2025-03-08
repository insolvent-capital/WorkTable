use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::persistence::PersistenceEngineOps;
use crate::prelude::Operation;

#[derive(Debug)]
pub struct Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    queue: lockfree::queue::Queue<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
    notify: Notify,
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
    Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    pub fn new() -> Self {
        Self {
            queue: lockfree::queue::Queue::new(),
            notify: Notify::new(),
        }
    }

    pub fn push(&self, value: Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>) {
        self.queue.push(value);
        self.notify.notify_one();
    }

    pub async fn pop(&self) -> Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
        loop {
            // Drain values
            if let Some(value) = self.queue.pop() {
                return value;
            }

            // Wait for values to be available
            self.notify.notified().await;
        }
    }

    pub fn immediate_pop(
        &self,
    ) -> Option<Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>> {
        self.queue.pop()
    }
}

#[derive(Debug)]
pub struct PersistenceTask<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    #[allow(dead_code)]
    engine_task_handle: tokio::task::AbortHandle,
    queue: Arc<Queue<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>>,
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
        SecondaryKeys: Debug + Send + 'static,
        PrimaryKeyGenState: Debug + Send + 'static,
        PrimaryKey: Debug + Send + 'static,
    {
        let queue = Arc::new(Queue::new());
        let progress_notify = Arc::new(Notify::new());

        let engine_queue = queue.clone();
        let engine_progress_notify = progress_notify.clone();
        let task = async move {
            loop {
                let next_op = if let Some(next_op) = engine_queue.immediate_pop() {
                    next_op
                } else {
                    engine_progress_notify.notify_waiters();
                    engine_queue.pop().await
                };
                let res = engine.apply_operation(next_op);
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
        }
    }

    pub async fn wait_for_ops(&self) {
        self.progress_notify.notified().await
    }
}
