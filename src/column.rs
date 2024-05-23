use crate::ty::Type;
use crate::value::{Value, ValueRef, ValueRefMut};
pub(crate) type ColumnId = u8;

pub(crate) trait IntoColumn: Sized {
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
            (this, value) => panic!(
                "Cannot push column of different type: {:?} vs {:?}",
                this.get_type(),
                value
            ),
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
    pub fn clear(&mut self) {
        match self {
            Column::Int(x) => {
                x.clear();
            }
            Column::String(x) => {
                x.clear();
            }
            Column::Float(x) => {
                x.clear();
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
