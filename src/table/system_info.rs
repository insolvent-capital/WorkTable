use data_bucket::Link;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::in_memory::{RowWrapper, StorableRow};
use crate::{IndexMap, IndexMultiMap, WorkTable};

#[derive(Debug)]
pub struct SystemInfo {
    pub table_name: &'static str,
    pub page_count: usize,
    pub row_count: usize,
    pub empty_slots: u64,
    pub memory_usage_bytes: u64,
    pub idx_size: usize,
}

pub trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map_or(0, |v| v.heap_size())
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        self.capacity() * std::mem::size_of::<T>()
            + self.iter().map(|v| v.heap_size()).sum::<usize>()
    }
}

impl HeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

impl<T, V> HeapSize for IndexMap<T, V>
where
    T: Ord + Clone + 'static + HeapSize + std::marker::Send,
    V: Ord + Clone + 'static + HeapSize + std::marker::Send,
{
    fn heap_size(&self) -> usize {
        let mut size = std::mem::size_of_val(self);

        for (k, v) in self.iter() {
            size += k.heap_size();
            size += v.heap_size();
        }

        size
    }
}

impl<T, V> HeapSize for IndexMultiMap<T, V>
where
    T: Ord + Clone + 'static + HeapSize + std::marker::Send,
    V: Ord + Clone + 'static + HeapSize + std::marker::Send,
{
    fn heap_size(&self) -> usize {
        let mut size = std::mem::size_of_val(self);

        for (k, v) in self.iter() {
            size += k.heap_size();
            size += v.heap_size();
        }

        size
    }
}

impl HeapSize for Link {
    fn heap_size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

impl<T: HeapSize> HeapSize for Box<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
}

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
}

impl<T: HeapSize> HeapSize for Rc<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + (**self).heap_size()
    }
}

impl<K: HeapSize + Eq + std::hash::Hash, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let mut size = 0;
        for (k, v) in self.iter() {
            size += k.heap_size();
            size += v.heap_size();
        }
        size
    }
}

impl HeapSize for u8 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for u16 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for u32 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for i32 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for u64 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for i64 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for f64 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for f32 {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for isize {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for bool {
    fn heap_size(&self) -> usize {
        0
    }
}
impl HeapSize for char {
    fn heap_size(&self) -> usize {
        0
    }
}

impl<
        Row,
        PrimaryKey,
        AvailableTypes,
        SecondaryIndexes: HeapSize,
        LockType,
        PkGen,
        const DATA_LENGTH: usize,
    > WorkTable<Row, PrimaryKey, AvailableTypes, SecondaryIndexes, LockType, PkGen, DATA_LENGTH>
where
    PrimaryKey: Clone + Ord + Send + 'static + std::hash::Hash,
    Row: StorableRow,
    <Row as StorableRow>::WrappedRow: RowWrapper<Row>,
{
    pub fn system_info(&self) -> SystemInfo {
        let page_count = self.data.get_page_count();
        let row_count = self.pk_map.len();

        let empty_links = self.data.get_empty_links().len();

        let bytes = self.data.get_bytes();

        let memory_usage_bytes = bytes
            .iter()
            .map(|(_buf, free_offset)| *free_offset as u64)
            .sum();

        let idx_size = self.indexes.heap_size();

        SystemInfo {
            table_name: self.table_name,
            page_count,
            row_count,
            empty_slots: empty_links as u64,
            memory_usage_bytes,
            idx_size,
        }
    }
}

impl SystemInfo {
    pub fn pretty(&self) -> String {
        let mem_kb = self.memory_usage_bytes as f64 / 1024.0;
        let idx_kb = self.idx_size as f64 / 1024.0;

        format!(
            "\
|| Table: {}\n\
|| Rows: {} ({} pages, {} empty slots)\n\
|| Memory: {:.2} KB (data) + {:.2} KB (indexes)\n\
|| Total: {:.2} KB",
            self.table_name,
            self.row_count,
            self.page_count,
            self.empty_slots,
            mem_kb,
            idx_kb,
            mem_kb + idx_kb,
        )
    }
}
