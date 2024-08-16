use std::collections::BTreeMap;
use std::io::Read;
use std::iter::IntoIterator;
use std::sync::Arc;

use eyre::{Context, Result};
use tokio::sync::RwLock;

use crate::column::{Column, ColumnId};
use crate::value::Value;
use crate::{IntoColumn, RowInsertion, RowView, RowViewMut, WorkTableField};

pub struct WorkTable {
    pub(crate) column_names: Vec<String>,
    pub(crate) columns_map: BTreeMap<String, ColumnId>,
    pub(crate) column_values: Vec<Column>,

    pub(crate) primary_map: Option<BTreeMap<Value, usize>>,
}
impl Default for WorkTable {
    fn default() -> Self {
        Self::new()
    }
}
impl WorkTable {
    pub fn new() -> Self {
        Self {
            column_names: vec![],
            columns_map: Default::default(),
            column_values: vec![],
            primary_map: None,
        }
    }
    pub fn set_primary(&mut self) {
        // index must be first column
        let column = 0;
        let mut index_values = BTreeMap::new();
        for (i, value) in self.column_values[column].iter().enumerate() {
            index_values.insert(value.clone(), i);
        }
        self.primary_map = Some(index_values);
    }
    pub fn add_column(&mut self, name: impl Into<String>, column: Column) {
        let name = name.into();
        let exists = self.columns_map.contains_key(&name);
        if exists {
            panic!("Overwriting column {}", name);
        }

        self.columns_map
            .insert(name.clone(), self.column_names.len() as _);
        self.column_names.push(name);
        self.column_values.push(column);
    }
    //noinspection RsConstantConditionIf
    pub fn add_field<T: WorkTableField>(&mut self, _field: T) {
        let name = T::NAME.to_string();
        let index = self.column_names.len();
        assert_eq!(index, T::INDEX, "Field index mismatch");
        self.columns_map.insert(name.clone(), index as _);
        self.column_names.push(name);
        self.column_values.push(T::Type::into_column());

        if T::PRIMARY {
            self.set_primary();
        }
    }
    pub fn push<const N: usize>(&mut self, row: [Value; N]) {
        assert_eq!(self.column_values.len(), N);
        if let Some(primary_map) = &mut self.primary_map {
            let index = primary_map.len();
            primary_map.insert(row[0].clone(), index);
        }
        for (i, column) in row.into_iter().enumerate() {
            self.column_values[i].push(column);
        }
    }

    pub fn insert(&mut self) -> RowInsertion {
        RowInsertion {
            values: vec![Value::Null; self.column_values.len()],
            table: self,
        }
    }

    pub fn len(&self) -> usize {
        if self.column_values.is_empty() {
            return 0;
        }
        self.column_values[0].len()
    }
    pub fn count_columns(&self) -> usize {
        self.column_values.len()
    }
    pub fn columns(&self) -> impl Iterator<Item = (&str, &Column)> {
        self.column_names
            .iter()
            .zip(self.column_values.iter())
            .map(|(name, column)| (name.as_str(), column))
    }

    pub fn shape(&self) -> (usize, usize) {
        let rows = self.len();
        let columns = self.count_columns();
        (rows, columns)
    }
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.column_names
            .iter()
            .enumerate()
            .find(|(_, x)| x == &name)
            .map(|(i, _)| &self.column_values[i])
    }

    pub fn get(&self, index: usize) -> Option<RowView> {
        if index >= self.shape().0 {
            return None;
        }
        Some(RowView { index, table: self })
    }
    pub fn index(&self, index: usize) -> RowView {
        self.get(index).expect("Index out of bounds")
    }
    pub fn get_mut(&mut self, index: usize) -> Option<RowViewMut> {
        if index >= self.shape().0 {
            return None;
        }
        Some(RowViewMut {
            index,
            table: self,
            begin: None,
        })
    }
    pub fn index_mut(&mut self, index: usize) -> RowViewMut {
        self.get_mut(index).expect("Index out of bounds")
    }
    pub fn get_by_primary(&self, index: &Value) -> Option<RowView> {
        let index = *self.primary_map.as_ref().expect("no index").get(index)?;
        Some(RowView { index, table: self })
    }
    pub fn get_by_primary_mut(&mut self, index: &Value) -> Option<RowViewMut> {
        let index = *self.primary_map.as_ref().expect("no index").get(index)?;
        Some(RowViewMut {
            index,
            table: self,
            begin: None,
        })
    }
    pub fn load_csv(&mut self, file: impl Read) -> Result<()> {
        let mut rdr = csv::Reader::from_reader(file);
        let headers = rdr.headers()?;
        for i in 0..headers.len() {
            debug_assert!(self.column_names[i] == headers[i]);
        }
        for result in rdr.records() {
            // The iterator yields Result<StringRecord, Error>, so we check the
            // error here.
            let record = result?;
            for (i, val) in record.iter().enumerate() {
                let col = &mut self.column_values[i];
                match col {
                    Column::Int(x) => {
                        let val = val.parse::<i64>().context("Parse error")?;
                        x.push(val);
                    }
                    Column::String(x) => {
                        x.push(val.to_string());
                    }
                    Column::Float(x) => {
                        let val = val.parse::<f64>().context("Parse error")?;
                        x.push(val);
                    }
                }
            }
        }
        Ok(())
    }
    pub fn sort_by_column(&mut self, column: &str) {
        debug_assert!(self.primary_map.is_none());
        let column = self.get_column(column).expect("Column not found");
        let mut indices: Vec<usize> = (0..column.len()).collect();
        indices.sort_by_key(|a| column.get_value(*a));
        for col in &mut self.column_values {
            // indices.iter().map(|i| col.get(i).unwrap()).collect();
            let new_columns = match col {
                Column::Int(x) => Column::Int(indices.iter().map(|i| x[*i]).collect()),
                Column::String(x) => {
                    Column::String(indices.iter().map(|i| x[*i].clone()).collect())
                }
                Column::Float(x) => Column::Float(indices.iter().map(|i| x[*i]).collect()),
            };
            *col = new_columns;
        }
    }
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = RowView> {
        let len = self.len();
        (0..len).map(|index| RowView { index, table: self })
    }
    // returns a mutable iterator over rows
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = RowViewMut> {
        struct IterMut<'a> {
            begin: usize,
            end: usize,
            table: *mut WorkTable,
            phantom: std::marker::PhantomData<&'a mut WorkTable>,
        }
        impl<'a> Iterator for IterMut<'a> {
            type Item = RowViewMut<'a>;
            fn next(&mut self) -> Option<Self::Item> {
                let table = unsafe { &mut *self.table };
                // handle row removal
                self.end = self.end.min(table.len());
                if self.begin >= self.end {
                    return None;
                }
                let row = RowViewMut {
                    index: self.begin,
                    table,
                    // handle row removal
                    begin: Some(&mut self.begin),
                };
                self.begin += 1;
                Some(row)
            }
        }
        impl<'a> DoubleEndedIterator for IterMut<'a> {
            fn next_back(&mut self) -> Option<Self::Item> {
                if self.end == 0 {
                    return None;
                }
                self.end -= 1;
                let table = unsafe { &mut *self.table };
                Some(RowViewMut {
                    index: self.end,
                    table,
                    begin: None,
                })
            }
        }
        IterMut {
            begin: 0,
            end: self.len(),
            table: self,
            phantom: std::marker::PhantomData,
        }
    }
    pub fn retain(&mut self, f: impl Fn(&RowViewMut) -> bool) {
        // remove from reverse order is generally faster
        self.iter_mut().rev().filter(|x| !f(x)).for_each(|row| {
            row.remove();
        });
    }
    pub fn clear(&mut self) {
        self.column_values.iter_mut().for_each(|x| x.clear());
        if let Some(primary_map) = &mut self.primary_map {
            primary_map.clear();
        }
    }
}

pub type SyncWorkTable = Arc<RwLock<WorkTable>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field;

    field!(0, PrimaryA: i64, "ia", primary = true);
    field!(0, A: i64, "a");
    field!(1, B: String, "b");
    field!(2, C: f64, "c");
    #[test]
    fn test_worktable_insert() {
        let mut table = WorkTable::new();
        table.add_field(A);
        table.add_field(B);
        table.add_field(C);

        // stream API
        table
            .insert()
            .set(A, 1)
            .set(B, "a".to_string())
            .set(C, 1.0)
            .finish();
        table.push([2.into(), "b".into(), 2.0.into()]);
        table.push([3.into(), "c".into(), 3.0.into()]);
        assert_eq!(table.shape(), (3, 3));
        assert_eq!(table.index(0).index(A), &1);
        assert_eq!(table.index(0).index(B), "a");
        assert_eq!(table.index(0).index(C), &1.0);
        assert_eq!(table.index(1).index(A), &2);
        assert_eq!(table.index(1).index(B), "b");
        assert_eq!(table.index(1).index(C), &2.0);
        assert_eq!(table.index(2).index(A), &3);
        assert_eq!(table.index(2).index(B), "c");
        assert_eq!(table.index(2).index(C), &3.0);
    }
    #[test]
    fn test_worktable_remove() {
        let mut table = WorkTable::new();

        table.add_field(A);
        table.add_field(B);
        table.add_field(C);
        table.push([1.into(), "a".into(), 1.0.into()]);
        table.push([2.into(), "b".into(), 2.0.into()]);
        table.push([3.into(), "c".into(), 3.0.into()]);
        table.index_mut(1).remove();
        assert_eq!(table.shape(), (2, 3));
        assert_eq!(table.index(0).index(A), &1);
        assert_eq!(table.index(0).index(B), "a");
        assert_eq!(table.index(0).index(C), &1.0);
        assert_eq!(table.index(1).index(A), &3);
        assert_eq!(table.index(1).index(B), "c");
        assert_eq!(table.index(1).index(C), &3.0);
    }
    #[test]
    fn test_worktable_update() {
        let mut table = WorkTable::new();
        table.add_field(A);
        table.add_field(B);
        table.add_field(C);
        table.push([1.into(), "a".into(), 1.0.into()]);
        table.push([2.into(), "b".into(), 2.0.into()]);
        table.push([3.into(), "c".into(), 3.0.into()]);
        *table.index_mut(1).index_mut(A) = 4;
        table.index_mut(1).index_mut(B).push_str("b");
        *table.index_mut(1).index_mut(C) = 4.0;
        assert_eq!(table.shape(), (3, 3));
        assert_eq!(table.index(0).index(A), &1);
        assert_eq!(table.index(0).index(B), "a");
        assert_eq!(table.index(0).index(C), &1.0);
        assert_eq!(table.index(1).index(A), &4);
        assert_eq!(table.index(1).index(B), "bb");
        assert_eq!(table.index(1).index(C), &4.0);
        assert_eq!(table.index(2).index(A), &3);
        assert_eq!(table.index(2).index(B), "c");
        assert_eq!(table.index(2).index(C), &3.0);
    }

    #[test]
    fn test_worktable_index() {
        let mut table = WorkTable::new();
        table.add_field(PrimaryA);
        table.add_field(B);
        table.add_field(C);
        table.push([1.into(), "a".into(), 1.0.into()]);
        table.push([2.into(), "b".into(), 2.0.into()]);
        table.push([3.into(), "c".into(), 3.0.into()]);
        assert_eq!(table.shape(), (3, 3));
        assert_eq!(table.get_by_primary(&1.into()).unwrap().index(A), &1);
        assert_eq!(table.get_by_primary(&1.into()).unwrap().index(B), "a");
        assert_eq!(table.get_by_primary(&1.into()).unwrap().index(C), &1.0);
        assert_eq!(table.get_by_primary(&2.into()).unwrap().index(A), &2);
        assert_eq!(table.get_by_primary(&2.into()).unwrap().index(B), "b");
        assert_eq!(table.get_by_primary(&2.into()).unwrap().index(C), &2.0);
        assert_eq!(table.get_by_primary(&3.into()).unwrap().index(A), &3);
        assert_eq!(table.get_by_primary(&3.into()).unwrap().index(B), "c");
        assert_eq!(table.get_by_primary(&3.into()).unwrap().index(C), &3.0);
    }
}
