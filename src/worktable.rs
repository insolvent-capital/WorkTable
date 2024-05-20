use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::iter::IntoIterator;
use std::sync::Arc;

use eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Type {
    Int,
    String,
    Float,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    String(String),
    Float(f64),
}
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
enum ValueRef<'a> {
    Int(&'a i64),
    String(&'a String),
    Float(&'a f64),
}

#[derive(Debug, Serialize, PartialEq)]
enum ValueRefMut<'a> {
    Int(&'a mut i64),
    String(&'a mut String),
    Float(&'a mut f64),
}
// assuming non-nan floats
impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::String(x), Value::String(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap(),
            _ => panic!("Cannot compare {:?} and {:?}", self, other),
        }
    }
}
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Int(x) => x.hash(state),
            Value::String(x) => x.hash(state),
            Value::Float(x) => x.to_bits().hash(state),
        }
    }
}

impl From<Value> for i64 {
    fn from(val: Value) -> Self {
        match val {
            Value::Int(x) => x,
            _ => panic!("Cannot convert {:?} to i64", val),
        }
    }
}
impl<'a> From<ValueRef<'a>> for &'a i64 {
    fn from(val: ValueRef<'a>) -> Self {
        match val {
            ValueRef::Int(x) => x,
            _ => panic!("Cannot convert {:?} to i64", val),
        }
    }
}
impl<'a> From<ValueRefMut<'a>> for &'a mut i64 {
    fn from(val: ValueRefMut<'a>) -> Self {
        match val {
            ValueRefMut::Int(x) => x,
            _ => panic!("Cannot convert {:?} to i64", val),
        }
    }
}

impl From<Value> for String {
    fn from(val: Value) -> Self {
        match val {
            Value::String(x) => x,
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}
impl<'a> From<ValueRef<'a>> for &'a String {
    fn from(val: ValueRef<'a>) -> Self {
        match val {
            ValueRef::String(x) => x,
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}
impl<'a> From<ValueRef<'a>> for &'a str {
    fn from(val: ValueRef<'a>) -> Self {
        match val {
            ValueRef::String(x) => x,
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}
impl<'a> From<ValueRefMut<'a>> for &'a mut String {
    fn from(val: ValueRefMut<'a>) -> Self {
        match val {
            ValueRefMut::String(x) => x,
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}

impl From<Value> for f64 {
    fn from(val: Value) -> Self {
        match val {
            Value::Float(x) => x,
            _ => panic!("Cannot convert {:?} to f64", val),
        }
    }
}
impl<'a> From<ValueRef<'a>> for &'a f64 {
    fn from(val: ValueRef<'a>) -> Self {
        match val {
            ValueRef::Float(x) => x,
            _ => panic!("Cannot convert {:?} to f64", val),
        }
    }
}
impl<'a> From<ValueRefMut<'a>> for &'a mut f64 {
    fn from(val: ValueRefMut<'a>) -> Self {
        match val {
            ValueRefMut::Float(x) => x,
            _ => panic!("Cannot convert {:?} to f64", val),
        }
    }
}

impl From<i64> for Value {
    fn from(x: i64) -> Self {
        Value::Int(x)
    }
}
impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}
impl From<f64> for Value {
    fn from(x: f64) -> Self {
        Value::Float(x)
    }
}
impl<'a> From<&'a str> for Value {
    fn from(x: &str) -> Self {
        Value::String(x.to_string())
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = eyre::Report;
    fn try_from(x: serde_json::Value) -> Result<Self, Self::Error> {
        match x {
            serde_json::Value::Number(x) => {
                if let Some(x) = x.as_i64() {
                    Ok(Value::Int(x))
                } else if let Some(x) = x.as_f64() {
                    Ok(Value::Float(x))
                } else {
                    Err(eyre!("Cannot convert json number {} to Value", x))
                }
            }
            serde_json::Value::String(x) => Ok(Value::String(x)),
            _ => Err(eyre!("Cannot convert json value {} to Value", x)),
        }
    }
}
trait IntoColumn: Sized {
    fn into_column() -> Column;
}
impl IntoColumn for i64 {
    fn into_column() -> Column {
        Column::Int(vec![])
    }
}
impl IntoColumn for String {
    fn into_column() -> Column {
        Column::String(vec![])
    }
}
impl IntoColumn for f64 {
    fn into_column() -> Column {
        Column::Float(vec![])
    }
}

pub enum Column {
    Int(Vec<i64>),
    String(Vec<String>),
    Float(Vec<f64>),
}
impl Column {
    pub fn push(&mut self, value: Value) {
        match (self, value) {
            (Column::Int(x), Value::Int(y)) => x.push(y),
            (Column::String(x), Value::String(y)) => x.push(y),
            (Column::Float(x), Value::Float(y)) => x.push(y),
            _ => panic!("Cannot push column of different type"),
        }
    }
    pub fn extend(&mut self, value: impl Into<Column>) {
        match (self, value.into()) {
            (Column::Int(x), Column::Int(y)) => x.extend(y),
            (Column::String(x), Column::String(y)) => x.extend(y),
            (Column::Float(x), Column::Float(y)) => x.extend(y),
            _ => panic!("Cannot push column of different type"),
        }
    }
    pub fn get_value(&self, index: usize) -> Option<Value> {
        let val = match self {
            Column::Int(x) => Value::Int(x.get(index)?.clone()),
            Column::String(x) => Value::String(x.get(index)?.clone()),
            Column::Float(x) => Value::Float(x.get(index)?.clone()),
        };
        Some(val)
    }

    #[allow(private_bounds)]
    pub fn get<T: ?Sized>(&self, index: usize) -> Option<&T>
    where
        for<'b> &'b T: From<ValueRef<'b>>,
    {
        let x = match self {
            Column::Int(x) => ValueRef::Int(x.get(index)?),
            Column::String(x) => ValueRef::String(x.get(index)?),
            Column::Float(x) => ValueRef::Float(x.get(index)?),
        };
        Some(x.into())
    }
    #[allow(private_bounds)]
    pub fn get_mut<T: ?Sized>(&mut self, index: usize) -> Option<&mut T>
    where
        for<'b> &'b mut T: From<ValueRefMut<'b>>,
    {
        let x = match self {
            Column::Int(x) => ValueRefMut::Int(x.get_mut(index)?),
            Column::String(x) => ValueRefMut::String(x.get_mut(index)?),
            Column::Float(x) => ValueRefMut::Float(x.get_mut(index)?),
        };
        Some(x.into())
    }
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        match self {
            Column::Int(x) => x.get(index).copied(),
            _ => panic!("Cannot get i64 from column of different type"),
        }
    }
    pub fn get_str(&self, index: usize) -> Option<&str> {
        match self {
            Column::String(x) => x.get(index).map(|x| x.as_str()),
            _ => panic!("Cannot get String from column of different type"),
        }
    }
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        match self {
            Column::Float(x) => x.get(index).copied(),
            _ => panic!("Cannot get f64 from column of different type"),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Column::Int(x) => x.len(),
            Column::String(x) => x.len(),
            Column::Float(x) => x.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Column::Int(x) => x.is_empty(),
            Column::String(x) => x.is_empty(),
            Column::Float(x) => x.is_empty(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Value> + '_ {
        ColumnIterator {
            column: self,
            index: 0,
        }
    }
    pub fn get_type(&self) -> Type {
        match self {
            Column::Int(_) => Type::Int,
            Column::String(_) => Type::String,
            Column::Float(_) => Type::Float,
        }
    }
    pub fn as_i64(&self) -> &[i64] {
        match self {
            Column::Int(x) => x.as_slice(),
            _ => panic!(
                "Cannot get i64 from column of different type: {:?}",
                self.get_type()
            ),
        }
    }
    pub fn as_str(&self) -> &[String] {
        match self {
            Column::String(x) => x.as_slice(),
            _ => panic!(
                "Cannot get String from column of different type: {:?}",
                self.get_type()
            ),
        }
    }
    pub fn as_f64(&self) -> &[f64] {
        match self {
            Column::Float(x) => x.as_slice(),
            _ => panic!(
                "Cannot get f64 from column of different type: {:?}",
                self.get_type()
            ),
        }
    }
    pub fn swap_remove(&mut self, index: usize) {
        match self {
            Column::Int(x) => {
                x.swap_remove(index);
            }
            Column::String(x) => {
                x.swap_remove(index);
            }
            Column::Float(x) => {
                x.swap_remove(index);
            }
        };
    }
}

struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
}
impl Iterator for ColumnIterator<'_> {
    type Item = Value;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.column.get_value(self.index)?;
        self.index += 1;
        Some(value)
    }
}

#[derive(Clone)]
pub struct RowView<'a> {
    index: usize,
    table: &'a WorkTable,
}
impl<'a> RowView<'a> {
    #[allow(private_bounds)]
    pub fn index<T>(&self, _field: T) -> &T::Type
    where
        T: WorkTableField,
        for<'b> &'b T::Type: From<ValueRef<'b>>,
    {
        self.table.column_values[T::INDEX].get(self.index).unwrap()
    }
    #[allow(private_bounds)]
    pub fn get<T: ?Sized>(&self, column: &str) -> Option<&T>
    where
        for<'b> &'b T: From<ValueRef<'b>>,
    {
        let column = self.table.columns_map.get(column)?;

        self.table.column_values[*column as usize].get(self.index)
    }
    pub fn dump(&self) -> Vec<Value> {
        self.table
            .column_names
            .iter()
            .enumerate()
            .map(|(i, _)| self.table.column_values[i].get_value(self.index).unwrap())
            .collect()
    }
}
pub struct RowViewMut<'a> {
    index: usize,
    table: &'a mut WorkTable,
    begin: Option<*mut usize>,
}
impl<'a> RowViewMut<'a> {
    #[allow(private_bounds)]
    pub fn index<T>(&self, _field: T) -> &T::Type
    where
        T: WorkTableField,
        for<'b> &'b T::Type: From<ValueRef<'b>>,
    {
        self.table.column_values[T::INDEX].get(self.index).unwrap()
    }
    #[allow(private_bounds)]
    pub fn get<T: ?Sized>(&self, column: &str) -> Option<&T>
    where
        for<'b> &'b T: From<ValueRef<'b>>,
    {
        let column = self.table.columns_map.get(column)?;

        self.table.column_values[*column as usize].get(self.index)
    }
    #[allow(private_bounds)]
    pub fn index_mut<T>(&mut self, _field: T) -> &mut T::Type
    where
        T: WorkTableField,
        for<'b> &'b mut T::Type: From<ValueRefMut<'b>>,
    {
        self.table.column_values[T::INDEX]
            .get_mut(self.index)
            .unwrap()
    }
    #[allow(private_bounds)]
    pub fn get_mut<T: ?Sized>(&mut self, column: &str) -> Option<&mut T>
    where
        for<'b> &'b mut T: From<ValueRefMut<'b>>,
    {
        let column = *self.table.columns_map.get(column)?;
        debug_assert_ne!(
            self.table
                .primary_map
                .as_ref()
                .map_or(ColumnId::MAX, |_| column),
            0,
            "Cannot get mutable reference to index column"
        );
        self.table.column_values[column as usize].get_mut(self.index)
    }
    pub fn remove(self) {
        let len = self.table.len();
        if let Some(index_values) = &mut self.table.primary_map {
            let index_value = self.table.column_values[0].get_value(self.index).unwrap();
            // special case for last element
            if self.index == len - 1 {
                index_values.remove(&index_value);
            } else {
                let last_index_value = self.table.column_values[0].get_value(len - 1).unwrap();
                let current_index_mapping = *index_values.get(&index_value).unwrap();
                index_values.remove(&index_value);
                index_values.insert(last_index_value, current_index_mapping);
            }
        }
        if let Some(begin) = self.begin {
            // update begin pointers
            unsafe {
                *begin = self.index;
            }
        }

        self.table
            .column_values
            .iter_mut()
            .for_each(|x| x.swap_remove(self.index));
    }
    pub fn dump(&self) -> Vec<Value> {
        self.table
            .column_names
            .iter()
            .enumerate()
            .map(|(i, _)| self.table.column_values[i].get_value(self.index).unwrap())
            .collect()
    }
}

type ColumnId = u8;
pub struct WorkTable {
    column_names: Vec<String>,
    columns_map: BTreeMap<String, ColumnId>,
    column_values: Vec<Column>,

    primary_map: Option<BTreeMap<Value, usize>>,
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
        self.add_column(T::NAME, <T::Type as IntoColumn>::into_column());

        if T::PRIMARY {
            self.set_primary();
        }
    }
    pub fn add_row<const N: usize>(&mut self, row: [Value; N]) {
        assert_eq!(self.column_values.len(), N);
        if let Some(primary_map) = &mut self.primary_map {
            let index = primary_map.len();
            primary_map.insert(row[0].clone(), index);
        }
        for (i, column) in row.into_iter().enumerate() {
            self.column_values[i].push(column);
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
}

pub type SyncWorkTable = Arc<RwLock<WorkTable>>;

pub trait WorkTableField {
    #[allow(private_bounds)]
    type Type: IntoColumn;
    const INDEX: usize;
    const NAME: &'static str;
    const PRIMARY: bool = false;
}
#[macro_export]
macro_rules! field {
    (
        $index: expr, $f: ident: $ty: ty, $name: expr $(, primary = $indexed: expr)?
    ) => {
        pub struct $f;
        impl WorkTableField for $f {
            type Type = $ty;
            const INDEX: usize = $index;
            const NAME: &'static str = $name;
            $(const PRIMARY: bool = $indexed;)? // optional
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

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
        table.add_row([1.into(), "a".into(), 1.0.into()]);
        table.add_row([2.into(), "b".into(), 2.0.into()]);
        table.add_row([3.into(), "c".into(), 3.0.into()]);
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
        table.add_row([1.into(), "a".into(), 1.0.into()]);
        table.add_row([2.into(), "b".into(), 2.0.into()]);
        table.add_row([3.into(), "c".into(), 3.0.into()]);
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
        table.add_row([1.into(), "a".into(), 1.0.into()]);
        table.add_row([2.into(), "b".into(), 2.0.into()]);
        table.add_row([3.into(), "c".into(), 3.0.into()]);
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
        table.add_row([1.into(), "a".into(), 1.0.into()]);
        table.add_row([2.into(), "b".into(), 2.0.into()]);
        table.add_row([3.into(), "c".into(), 3.0.into()]);
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
