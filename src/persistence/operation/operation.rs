use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use data_bucket::Link;
use indexset::cdc::change::ChangeEvent;
use indexset::core::pair::Pair;

use crate::persistence::{OperationId, OperationType};

#[derive(Clone, Debug)]
pub enum Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> {
    Insert(InsertOperation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>),
    Update(UpdateOperation<SecondaryKeys>),
    Delete(DeleteOperation<PrimaryKey, SecondaryKeys>),
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> Hash
    for Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.operation_id(), state)
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> PartialEq
    for Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
    fn eq(&self, other: &Self) -> bool {
        self.operation_id().eq(&other.operation_id())
    }
}

impl<PrimaryKeyGenState, PrimaryKey, SecondaryKeys> Eq
    for Operation<PrimaryKeyGenState, PrimaryKey, SecondaryKeys>
{
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
