use eyre::eyre;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    String(String),
    Float(f64),
}
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) enum ValueRef<'a> {
    Int(&'a i64),
    String(&'a String),
    Float(&'a f64),
}

#[derive(Debug, Serialize, PartialEq)]
pub(crate) enum ValueRefMut<'a> {
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
            Value::Null => 0.hash(state),
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
    fn try_from(x: serde_json::Value) -> eyre::Result<Self, Self::Error> {
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
