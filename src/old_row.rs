use std::mem::transmute;
use std::ops::Deref;

use crate::column::ColumnId;
use crate::old_table::WorkTable;
use crate::value::{Value, ValueRef, ValueRefMut};
use crate::WorkTableField;

#[derive(Clone)]
#[repr(C)]
pub struct RowView<'a> {
    pub index: usize,
    pub(crate) table: &'a WorkTable,
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
#[repr(C)]
pub struct RowViewMut<'a> {
    pub(crate) index: usize,
    pub(crate) table: &'a mut WorkTable,
    pub(crate) begin: Option<*mut usize>,
}
impl<'a> Deref for RowViewMut<'a> {
    type Target = RowView<'a>;
    fn deref(&self) -> &Self::Target {
        // SAFETY: see as_view
        unsafe { transmute(self) }
    }
}

impl<'a> RowViewMut<'a> {
    pub fn as_view(&self) -> &RowView {
        // SAFETY: first 2 fields of RowViewMut and RowView are the same
        // downgrades the write reference to read reference
        unsafe { transmute(self) }
    }
    pub fn to_view(&self) -> RowView {
        RowView {
            index: self.index,
            table: self.table,
        }
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
    pub fn set<T>(&mut self, field: T, value: T::Type)
    where
        T: WorkTableField,
        for<'b> &'b mut T::Type: From<ValueRefMut<'b>>,
    {
        *self.index_mut(field) = value;
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
}

#[must_use = "RowInsertion::finish() must be called to insert to the table"]
pub struct RowInsertion<'a> {
    pub(crate) values: Vec<Value>,
    pub(crate) table: &'a mut WorkTable,
}
impl RowInsertion<'_> {
    pub fn set<F>(mut self, _field: F, value: F::Type) -> Self
    where
        F: WorkTableField,
    {
        self.values[F::INDEX] = value.into();
        self
    }
    pub fn finish(self) {
        let len = self.table.len();
        self.table
            .column_values
            .iter_mut()
            .zip(self.values.into_iter())
            .for_each(|(x, y)| {
                if matches!(y, Value::Null) {
                    panic!("Cannot insert null value. check if your insertion is complete");
                }
                x.push(y)
            });
        if let Some(index_values) = &mut self.table.primary_map {
            let index_value = self.table.column_values[0].get_value(len).unwrap();
            index_values.insert(index_value, len);
        }
    }
}
