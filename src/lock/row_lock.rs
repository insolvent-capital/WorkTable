use crate::lock::Lock;
use std::collections::HashSet;
use std::sync::Arc;

pub trait RowLock {
    /// Checks if any column of this row is locked.
    fn is_locked(&self) -> bool;
    /// Creates new [`RowLock`] with all columns locked.
    fn with_lock(id: u16) -> (Self, Arc<Lock>)
    where
        Self: Sized;
    /// Locks full [`RowLock`].
    #[allow(clippy::mutable_key_type)]
    fn lock(&mut self, id: u16) -> (HashSet<Arc<Lock>>, Arc<Lock>);
    /// Merges two [`RowLock`]'s.
    #[allow(clippy::mutable_key_type)]
    fn merge(&mut self, other: &mut Self) -> HashSet<Arc<Lock>>
    where
        Self: Sized;
}
