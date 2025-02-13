mod set;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use derive_more::From;
use futures::task::AtomicWaker;
use rkyv::{Archive, Deserialize, Serialize};

pub use set::LockMap;

#[derive(
    Archive, Clone, Copy, Deserialize, Debug, Eq, From, Hash, Ord, Serialize, PartialEq, PartialOrd,
)]
pub struct LockId(u16);

#[derive(Debug)]
pub struct Lock {
    locked: AtomicBool,
    waker: AtomicWaker,
}

impl Future for &Lock {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_ref().waker.register(cx.waker());
        if self.locked.load(Ordering::Relaxed) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Default for Lock {
    fn default() -> Self {
        Self::new()
    }
}

impl Lock {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::from(true),
            waker: AtomicWaker::new(),
        }
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Relaxed);
        self.waker.wake()
    }
}
