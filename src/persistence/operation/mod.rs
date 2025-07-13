mod batch;
#[allow(clippy::module_inception)]
mod operation;
mod util;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use data_bucket::SizeMeasurable;
use derive_more::Display;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::prelude::From;

pub use batch::{BatchInnerRow, BatchInnerWorkTable, BatchOperation, PosByOpIdQuery};
pub use operation::{DeleteOperation, InsertOperation, Operation, UpdateOperation};
pub use util::validate_events;

/// Represents page's identifier. Is unique within the table bounds
#[derive(Archive, Copy, Clone, Deserialize, Debug, Display, From, Serialize)]
#[rkyv(derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
pub enum OperationId {
    #[from]
    Single(Uuid),
    Multi(Uuid),
}

impl OperationId {
    fn get_id(&self) -> Uuid {
        match self {
            OperationId::Single(id) => *id,
            OperationId::Multi(id) => *id,
        }
    }
}

impl Hash for OperationId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.get_id(), state)
    }
}

impl PartialEq for OperationId {
    fn eq(&self, other: &Self) -> bool {
        self.get_id().eq(&other.get_id())
    }
}

impl Eq for OperationId {}

impl PartialOrd for OperationId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OperationId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_id().cmp(&other.get_id())
    }
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
