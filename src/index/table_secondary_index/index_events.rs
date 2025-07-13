use crate::prelude::IndexChangeEventId;
use indexset::cdc::change;
use std::collections::HashMap;

pub trait TableSecondaryIndexEventsOps<AvailableIndexes> {
    fn extend(&mut self, another: Self)
    where
        Self: Sized;
    fn remove(&mut self, another: &Self)
    where
        Self: Sized;
    fn last_evs(&self) -> HashMap<AvailableIndexes, Option<IndexChangeEventId>>;
    fn first_evs(&self) -> HashMap<AvailableIndexes, Option<IndexChangeEventId>>;
    // TODO: Remove this when indexset will be fully fixed....................
    fn is_first_ev_is_split(&self, _index: AvailableIndexes) -> bool {
        false
    }
    fn iter_event_ids(&self) -> impl Iterator<Item = (AvailableIndexes, change::Id)>;
    fn contains_event(&self, _index: AvailableIndexes, _id: change::Id) -> bool {
        false
    }
    fn sort(&mut self);
    fn validate(&mut self) -> Self
    where
        Self: Sized;
    fn is_empty(&self) -> bool;
    fn is_unit() -> bool;
}
