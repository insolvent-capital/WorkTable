use std::fmt::{self, Display, Formatter};

use prettytable::{format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR, row, Table};

use crate::in_memory::{RowWrapper, StorableRow};
use crate::mem_stat::MemStat;
use crate::{TableSecondaryIndex, WorkTable};

#[derive(Debug)]
pub struct SystemInfo {
    pub table_name: &'static str,
    pub page_count: usize,
    pub row_count: usize,
    pub empty_slots: u64,
    pub memory_usage_bytes: u64,
    pub idx_size: usize,
    pub indexes_info: Vec<IndexInfo>,
}

#[derive(Debug)]
pub struct IndexInfo {
    pub name: String,
    pub index_type: IndexKind,
    pub key_count: usize,
    pub capacity: usize,
    pub heap_size: usize,
    pub used_size: usize,
    pub node_count: usize,
}

#[derive(Debug)]
pub enum IndexKind {
    Unique,
    NonUnique,
}

impl Display for IndexKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unique => write!(f, "unique"),
            Self::NonUnique => write!(f, "non unique"),
        }
    }
}

impl<
        Row,
        PrimaryKey,
        AvailableTypes,
        SecondaryIndexes: MemStat + TableSecondaryIndex<Row, AvailableTypes>,
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
            indexes_info: self.indexes.index_info(),
        }
    }
}

impl Display for SystemInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mem_fmt = fmt_bytes(self.memory_usage_bytes as usize);
        let idx_fmt = fmt_bytes(self.idx_size);
        let total_fmt = fmt_bytes(self.memory_usage_bytes as usize + self.idx_size);

        writeln!(f, "┌──────────────────────────────┐")?;
        writeln!(f, " \t Table Name: {:<5}", self.table_name)?;
        writeln!(f, "└──────────────────────────────┘")?;
        writeln!(
            f,
            "Rows: {}   Pages: {}   Empty slots: {}",
            self.row_count, self.page_count, self.empty_slots
        )?;
        writeln!(
            f,
            "Allocated Memory: {} (data) + {} (indexes) = {} total\n",
            mem_fmt, idx_fmt, total_fmt
        )?;

        let mut table = Table::new();
        table.set_format(*FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.add_row(row![
            "Index",
            "Type",
            "Keys",
            "Capacity",
            "Node Count",
            "Heap",
            "Used"
        ]);

        for idx in &self.indexes_info {
            table.add_row(row![
                idx.name,
                idx.index_type.to_string(),
                idx.key_count,
                idx.capacity,
                idx.node_count,
                fmt_bytes(idx.heap_size),
                fmt_bytes(idx.used_size),
            ]);
        }

        let mut buffer = Vec::new();
        table.print(&mut buffer).unwrap();
        let table_str = String::from_utf8(buffer).unwrap();
        writeln!(f, "{}", table_str.trim_end())?;

        Ok(())
    }
}

fn fmt_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;
    const GB: f64 = 1024.0 * MB;

    let b = bytes as f64;

    let (value, unit) = if b >= GB {
        (b / GB, "GB")
    } else if b >= MB {
        (b / MB, "MB")
    } else if b >= KB {
        (b / KB, "KB")
    } else {
        return format!("{} B", bytes);
    };

    if (value.fract() * 100.0).round() == 0.0 {
        format!("{:.0} {}", value, unit)
    } else {
        format!("{:.2} {}", value, unit)
    }
}
