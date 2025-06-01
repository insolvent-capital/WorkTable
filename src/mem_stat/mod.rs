mod primitives;

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use data_bucket::page::PageId;
use data_bucket::Link;
use indexset::core::multipair::MultiPair;
use indexset::core::node::NodeLike;
use indexset::core::pair::Pair;
use ordered_float::OrderedFloat;
use uuid::Uuid;

use crate::persistence::OperationType;
use crate::prelude::OperationId;
use crate::IndexMultiMap;
use crate::{impl_memstat_zero, IndexMap};

pub trait MemStat {
    fn heap_size(&self) -> usize;
    fn used_size(&self) -> usize;
}

impl<T: MemStat> MemStat for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.heap_size())
    }
    fn used_size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.used_size())
    }
}

impl<T: MemStat> MemStat for Vec<T> {
    fn heap_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
            + self.iter().map(|v| v.heap_size()).sum::<usize>()
    }
    fn used_size(&self) -> usize {
        self.len() * std::mem::size_of::<T>() + self.iter().map(|v| v.used_size()).sum::<usize>()
    }
}

impl MemStat for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
    fn used_size(&self) -> usize {
        self.len()
    }
}

impl<K, V, Node> MemStat for IndexMap<K, V, Node>
where
    K: Ord + Clone + 'static + MemStat + Send,
    V: Clone + 'static + MemStat + Send,
    Node: NodeLike<Pair<K, V>> + Send + 'static,
{
    fn heap_size(&self) -> usize {
        let slot_size = std::mem::size_of::<Pair<K, V>>();
        let base_heap = self.capacity() * slot_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }

    fn used_size(&self) -> usize {
        let pair_size = std::mem::size_of::<Pair<K, V>>();
        let base = self.len() * pair_size;

        let used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base + used
    }
}

impl<K, V, Node> MemStat for IndexMultiMap<K, V, Node>
where
    K: Ord + Clone + 'static + MemStat + Send,
    V: Ord + Clone + 'static + MemStat + Send,
    Node: NodeLike<MultiPair<K, V>> + Send + 'static,
{
    fn heap_size(&self) -> usize {
        let slot_size = std::mem::size_of::<MultiPair<K, V>>();
        let base_heap = self.capacity() * slot_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }

    fn used_size(&self) -> usize {
        let pair_size = std::mem::size_of::<MultiPair<K, V>>();
        let base = self.len() * pair_size;

        let used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base + used
    }
}

impl<T: MemStat> MemStat for Box<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<T: MemStat> MemStat for Arc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<T: MemStat> MemStat for Rc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
    fn used_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).used_size()
    }
}

impl<K: MemStat + Eq + std::hash::Hash, V: MemStat> MemStat for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let bucket_size = size_of::<(K, V)>();
        let base_heap = self.capacity() * bucket_size;

        let kv_heap: usize = self
            .iter()
            .map(|(k, v)| k.heap_size() + v.heap_size())
            .sum();

        base_heap + kv_heap
    }
    fn used_size(&self) -> usize {
        let bucket_size = size_of::<(K, V)>();
        let base_used = self.len() * bucket_size;

        let kv_used: usize = self
            .iter()
            .map(|(k, v)| k.used_size() + v.used_size())
            .sum();

        base_used + kv_used
    }
}

impl<T> MemStat for OrderedFloat<T>
where
    T: MemStat,
{
    fn heap_size(&self) -> usize {
        self.0.heap_size()
    }

    fn used_size(&self) -> usize {
        self.0.used_size()
    }
}

impl_memstat_zero!(Link, PageId, Uuid, OperationId, OperationType);
