mod map;
mod row_lock;

use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use derive_more::From;
use futures::task::AtomicWaker;

pub use map::LockMap;
pub use row_lock::RowLock;

#[derive(Debug)]
pub struct Lock {
    id: u16,
    locked: AtomicBool,
    waker: AtomicWaker,
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
            locked: AtomicBool::from(true),
            waker: AtomicWaker::new(),
        }
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
        self.waker.wake()
    }

    pub fn lock(&self) {
        self.locked.store(true, Ordering::Release);
        self.waker.wake()
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::Acquire)
    }
}

impl Future for &Lock {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_ref().waker.register(cx.waker());
        if self.locked.load(Ordering::Acquire) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
