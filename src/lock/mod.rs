mod map;
mod row_lock;

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use derive_more::From;
use futures::task::AtomicWaker;
pub use map::LockMap;
use parking_lot::Mutex;
pub use row_lock::RowLock;

#[derive(Debug)]
pub struct Lock {
    id: u16,
    locked: Arc<AtomicBool>,
    wakers: Mutex<Vec<Arc<AtomicWaker>>>,
}

impl PartialEq for Lock {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Lock {}

impl Hash for Lock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.id, state)
    }
}

impl Lock {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            locked: Arc::new(AtomicBool::from(true)),
            wakers: Mutex::new(vec![]),
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Relaxed);
        let guard = self.wakers.lock();
        for w in guard.iter() {
            w.wake()
        }
    }

    pub fn lock(&self) {
        self.locked.store(true, Ordering::Relaxed);
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Relaxed)
    }

    pub fn wait(&self) -> LockWait {
        let mut guard = self.wakers.lock();
        let waker = Arc::new(AtomicWaker::new());
        guard.push(waker.clone());
        LockWait {
            locked: self.locked.clone(),
            waker,
        }
    }
}

#[derive(Debug)]
pub struct LockWait {
    locked: Arc<AtomicBool>,
    waker: Arc<AtomicWaker>,
}

impl Future for LockWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.locked.load(Ordering::Relaxed) {
            return Poll::Ready(());
        }

        self.waker.register(cx.waker());

        if self.locked.load(Ordering::Relaxed) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
