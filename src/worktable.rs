use eyre::*;
use serde::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::iter::IntoIterator;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

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
// assuming non-nan floats
impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
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

impl From<Value> for String {
    fn from(val: Value) -> Self {
        match val {
            Value::String(x) => x,
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
    pub fn get<T: From<Value>>(&self, index: usize) -> Option<T> {
        let x = match self {
            Column::Int(x) => Value::Int(*x.get(index)?),
            Column::String(x) => Value::String(x.get(index)?.to_string()),
            Column::Float(x) => Value::Float(*x.get(index)?),
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
    pub fn update(&mut self, index: usize, value: Value) {
        match (self, value) {
            (Column::Int(x), Value::Int(y)) => x[index] = y,
            (Column::String(x), Value::String(y)) => x[index] = y,
            (Column::Float(x), Value::Float(y)) => x[index] = y,
            _ => panic!("Cannot update column of different type"),
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
}

struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
}
impl Iterator for ColumnIterator<'_> {
    type Item = Value;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.column.get(self.index)?;
        self.index += 1;
        Some(value)
    }
}

pub struct WorkTable {
    columns: Vec<String>,
    column_values: Vec<Column>,
    index: Option<usize>,
    index_values: HashMap<Value, usize>,
}
impl Default for WorkTable {
    fn default() -> Self {
        Self::new()
    }
}
impl WorkTable {
    pub fn new() -> Self {
        Self {
            columns: vec![],
            column_values: vec![],
            index: None,
            index_values: Default::default(),
        }
    }
    pub fn set_index(&mut self, index: &str) {
        if let Some(i) = self.index {
            warn!("Overwriting index {} {} with {}", self.columns[i], i, index);
        }
        let index = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, x)| x == &index)
            .expect("Index not found")
            .0;
        self.index = Some(index);
        let mut index_values = HashMap::new();
        for (i, row) in self.column_values[index].iter().enumerate() {
            index_values.insert(row, i);
        }
        self.index_values = index_values;
    }
    pub fn add_column(&mut self, name: impl Into<String>, column: Column) {
        let name = name.into();
        let exists = self.columns.iter().any(|x| x == &name);
        if exists {
            panic!("Overwriting column {}", name);
        }
        self.columns.push(name);
        self.column_values.push(column);
    }
    pub fn add_row<const N: usize>(&mut self, row: [Value; N]) {
        if let Some(index) = self.index {
            let index_value = row[index].clone();

            self.index_values
                .insert(index_value, self.column_values[0].len());
        }
        for (i, column) in row.into_iter().enumerate() {
            self.column_values[i].push(column);
        }
    }
    pub fn get_by_index(&self, index: &Value) -> Option<Vec<Value>> {
        let row = self.index_values.get(index)?;
        self.get_row(*row)
    }
    pub fn shape(&self) -> (usize, usize) {
        let columns = self.column_values.len();
        if columns == 0 {
            return (0, 0);
        }
        let rows = self.column_values[0].len();
        (rows, columns)
    }
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, x)| x == &name)
            .map(|(i, _)| &self.column_values[i])
    }
    pub fn get_column_i64(&self, name: &str) -> Option<&[i64]> {
        self.get_column(name).and_then(|x| match x {
            Column::Int(x) => Some(x.as_slice()),
            _ => None,
        })
    }
    pub fn get_column_str(&self, name: &str) -> Option<&[String]> {
        self.get_column(name).and_then(|x| match x {
            Column::String(x) => Some(x.as_slice()),
            _ => None,
        })
    }
    pub fn get_column_f64(&self, name: &str) -> Option<&[f64]> {
        self.get_column(name).and_then(|x| match x {
            Column::Float(x) => Some(x.as_slice()),
            _ => None,
        })
    }
    pub fn get_row(&self, index: usize) -> Option<Vec<Value>> {
        if index >= self.shape().0 {
            return None;
        }
        Some(
            self.column_values
                .iter()
                .map(|x| match x {
                    Column::Int(x) => Value::Int(x[index]),
                    Column::String(x) => Value::String(x[index].clone()),
                    Column::Float(x) => Value::Float(x[index]),
                })
                .collect(),
        )
    }
    pub fn get_value<T: From<Value>>(&self, column: &str, row: usize) -> Option<T> {
        let column = self.get_column(column)?;
        let value = column.get(row)?;
        Some(value)
    }
    pub fn get_value_i64(&self, column: &str, row: usize) -> Option<i64> {
        let column = self.get_column(column)?;
        let value = column.get_i64(row)?;
        Some(value)
    }
    pub fn get_value_str(&self, column: &str, row: usize) -> Option<&str> {
        let column = self.get_column(column)?;
        let value = column.get_str(row)?;
        Some(value)
    }
    pub fn get_value_f64(&self, column: &str, row: usize) -> Option<f64> {
        let column = self.get_column(column)?;
        let value = column.get_f64(row)?;
        Some(value)
    }
    pub fn get_value_by_index_i64(&self, index: &Value, column: &str) -> Option<i64> {
        let row = self.index_values.get(index)?;
        self.get_value_i64(column, *row)
    }
    pub fn get_value_by_index_str(&self, index: &Value, column: &str) -> Option<&str> {
        let row = self.index_values.get(index)?;
        self.get_value_str(column, *row)
    }
    pub fn get_value_by_index_f64(&self, index: &Value, column: &str) -> Option<f64> {
        let row = self.index_values.get(index)?;
        self.get_value_f64(column, *row)
    }
    pub fn update_value(&mut self, row: usize, column: &str, value: Value) {
        let column = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, x)| x == &column)
            .expect("Column not found")
            .0;
        self.column_values[column].update(row, value);
    }
    pub fn update_value_by_index(&mut self, index: &Value, column: &str, value: Value) {
        let row = self.index_values.get(index).expect("Index not found");
        self.update_value(*row, column, value);
    }
    pub fn load_csv(&mut self, file: impl Read) -> Result<()> {
        let mut rdr = csv::Reader::from_reader(file);
        let headers = rdr.headers()?;
        for i in 0..headers.len() {
            debug_assert!(self.columns[i] == headers[i]);
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
        debug_assert!(self.index.is_none());
        let column = self.get_column(column).expect("Column not found");
        let mut indices: Vec<usize> = (0..column.len()).collect();
        indices.sort_by_key(|a| column.get::<Value>(*a));
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
}
pub type SyncWorkTable = Arc<RwLock<WorkTable>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RWorkTable<T> {
    rows: Vec<T>,
}

impl<T> RWorkTable<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
        }
    }
    pub fn first<R>(&self, f: impl Fn(&T) -> R) -> Option<R> {
        self.rows.first().map(f)
    }
    pub fn rows(&self) -> &Vec<T> {
        &self.rows
    }
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.rows.iter()
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn into_result(self) -> Option<T> {
        self.rows.into_iter().next()
    }
    pub fn push(&mut self, row: T) {
        self.rows.push(row);
    }
    pub fn map<R>(self, f: impl Fn(T) -> R) -> Vec<R> {
        self.rows.into_iter().map(f).collect()
    }
    pub async fn map_async<R, F: Future<Output = Result<R>>>(
        self,
        f: impl Fn(T) -> F,
    ) -> Result<Vec<R>> {
        let mut futures = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            futures.push(f(row).await?);
        }
        Ok(futures)
    }
}

impl<T> IntoIterator for RWorkTable<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}
