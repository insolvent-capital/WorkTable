use data_bucket::{SizeMeasurable, UnsizedIndexPageUtility, VariableSizeMeasurable};
use indexset::core::node::NodeLike;

use std::borrow::Borrow;
use std::collections::Bound;
use std::fmt::Debug;
use std::ops::Deref;
use std::slice::Iter;

pub const UNSIZED_HEADER_LENGTH: u32 = 64;

#[derive(Debug, Clone)]
pub struct UnsizedNode<T>
where
    T: SizeMeasurable,
{
    inner: Vec<T>,
    length_capacity: usize,
    removed_length: usize,
    length: usize,
}

impl<T> AsRef<[T]> for UnsizedNode<T>
where
    T: SizeMeasurable,
{
    fn as_ref(&self) -> &[T] {
        self.inner.as_ref()
    }
}

impl<T> UnsizedNode<T>
where
    T: SizeMeasurable + Ord + Default + VariableSizeMeasurable,
{
    pub fn from_inner(inner: Vec<T>, length_capacity: usize) -> Self {
        let mut length = inner.last().unwrap().aligned_size();
        length += UNSIZED_HEADER_LENGTH as usize;
        for value in inner.iter() {
            length += value.aligned_size();
            length += UnsizedIndexPageUtility::<T>::slots_value_size();
        }

        Self {
            inner,
            length,
            length_capacity,
            removed_length: 0,
        }
    }

    pub fn rebuild(&mut self) {
        self.length = self
            .inner
            .last()
            .expect("should not rebuild on empty node")
            .aligned_size();
        self.length += UNSIZED_HEADER_LENGTH as usize;
        for value in self.inner.iter() {
            self.length += value.aligned_size();
            self.length += UnsizedIndexPageUtility::<T>::slots_value_size();
        }
        self.removed_length = 0;
    }
}

impl<T> NodeLike<T> for UnsizedNode<T>
where
    T: SizeMeasurable + Ord + Default + Debug + VariableSizeMeasurable,
{
    fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Vec::new(),
            length_capacity: capacity,
            length: UNSIZED_HEADER_LENGTH as usize,
            removed_length: 0,
        }
    }

    fn get_ith(&self, index: usize) -> Option<&T> {
        self.inner.get(index)
    }

    fn halve(&mut self) -> Self {
        let middle_length = (self.length
            - self.removed_length
            - (self.max().unwrap().aligned_size() + UNSIZED_HEADER_LENGTH as usize))
            / 2;
        let mut middle_variance = f64::INFINITY;
        let mut ind = false;
        let mut i = 1;
        let mut current_length = 0;
        let mut middle_idx = 0;
        let mut iter = self.inner.iter();
        while !ind {
            let Some(val) = iter.next() else {
                break;
            };
            current_length += val.aligned_size();
            current_length += UnsizedIndexPageUtility::<T>::slots_value_size();
            let current_middle_variance =
                (middle_length as f64 - current_length as f64) / (middle_length as f64);
            if current_middle_variance.abs() < middle_variance {
                middle_variance = current_middle_variance.abs();
                middle_idx = i;
            } else {
                ind = true;
                current_length -= val.aligned_size();
                current_length -= UnsizedIndexPageUtility::<T>::slots_value_size();
            }
            i += 1;
        }

        let new_inner = self.inner.split_off(middle_idx);
        let split = Self::from_inner(new_inner, self.length_capacity);
        self.rebuild();

        split
    }

    fn need_to_split(&self, _: usize, value: &T) -> bool {
        let final_length = self.length + value.aligned_size();
        final_length >= self.length_capacity && self.inner.len() > 1
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn capacity(&self) -> usize {
        self.length_capacity
    }

    fn insert(&mut self, value: T) -> (bool, usize) {
        let value_size = value.aligned_size();
        let node_id_len = self.max().map(|v| v.aligned_size()).unwrap_or(0);
        match NodeLike::insert(&mut self.inner, value) {
            (true, idx) => {
                if idx == self.inner.len() - 1 {
                    // Node id is stored separately too, so we need to count node_id twice
                    self.length -= node_id_len;
                    self.length += value_size;
                }
                self.length += value_size;
                self.length += UnsizedIndexPageUtility::<T>::slots_value_size();

                (true, idx)
            }
            (false, idx) => (false, idx),
        }
    }

    fn contains<Q: Ord + ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
    {
        NodeLike::contains(&self.inner, value)
    }

    fn try_select<Q: Ord + ?Sized>(&self, value: &Q) -> Option<usize>
    where
        T: Borrow<Q>,
    {
        NodeLike::try_select(&self.inner, value)
    }

    fn rank<Q: Ord + ?Sized>(&self, bound: Bound<&Q>, from_start: bool) -> Option<usize>
    where
        T: Borrow<Q>,
    {
        NodeLike::rank(&self.inner, bound, from_start)
    }

    fn delete<Q: Ord + ?Sized>(&mut self, value: &Q) -> Option<(T, usize)>
    where
        T: Borrow<Q>,
    {
        // TODO: Refactor this when empty links logic will be added to the page
        if let Some((val, i)) = NodeLike::delete(&mut self.inner, value) {
            self.removed_length +=
                val.aligned_size() + UnsizedIndexPageUtility::<T>::slots_value_size();

            if self.removed_length > self.length_capacity / 2 {
                self.rebuild()
            }
            Some((val, i))
        } else {
            None
        }
    }

    fn replace(&mut self, idx: usize, value: T) -> Option<T> {
        let value_size = value.aligned_size();
        if let Some(old) = self.inner.get_mut(idx) {
            let old = std::mem::replace(old, value);
            self.length += value_size;
            return Some(old);
        }

        None
    }

    fn max(&self) -> Option<&T> {
        self.inner.last()
    }

    fn pre_max(&self) -> Option<&T> {
        self.inner.pre_max()
    }

    fn min(&self) -> Option<&T> {
        self.inner.first()
    }

    fn iter<'a>(&'a self) -> Iter<'a, T>
    where
        T: 'a,
    {
        self.inner.deref().iter()
    }
}

#[cfg(test)]
mod test {
    use crate::index::unsized_node::UnsizedNode;
    use data_bucket::Link;
    use indexset::concurrent::multimap::BTreeMultiMap;
    use indexset::core::multipair::MultiPair;
    use indexset::core::node::NodeLike;

    #[test]
    fn test_split_basic() {
        let mut node = UnsizedNode::<String>::with_capacity(232);
        for i in 0..10 {
            let s = format!("{i}_______");
            node.insert(s);
        }
        assert_eq!(node.length, node.length_capacity);
        let split = node.halve();
        assert_eq!(node.inner.len(), split.inner.len());
        assert_eq!(node.length, split.length);
        assert_eq!(node.length, 152)
    }

    #[test]
    fn test_split() {
        let mut node = UnsizedNode::<String>::with_capacity(200);
        node.insert(String::from_utf8(vec![b'1'; 16]).unwrap());
        node.insert(String::from_utf8(vec![b'2'; 16]).unwrap());
        node.insert(String::from_utf8(vec![b'3'; 24]).unwrap());
        assert_eq!(node.length, node.length_capacity);
        let split = node.halve();
        assert_eq!(node.length, 152);
        assert_eq!(node.removed_length, 0);
        assert_eq!(node.inner.len(), 2);
        assert_eq!(split.length, 136);
        assert_eq!(split.removed_length, 0);
        assert_eq!(split.inner.len(), 1);
    }

    #[test]
    fn test_delete() {
        let mut node = UnsizedNode::<String>::with_capacity(200);
        node.insert(String::from_utf8(vec![b'1'; 16]).unwrap());
        assert_eq!(node.length, 120);
        assert_eq!(node.removed_length, 0);
        node.delete(&String::from_utf8(vec![b'1'; 16]).unwrap());
        assert_eq!(node.length, 120);
        assert_eq!(node.removed_length, 32);
    }

    #[test]
    fn test_delete_max_update() {
        let mut node = UnsizedNode::<String>::with_capacity(200);
        node.insert(String::from_utf8(vec![b'1'; 16]).unwrap());
        node.insert(String::from_utf8(vec![b'2'; 24]).unwrap());
        assert_eq!(node.length, 168);
        assert_eq!(node.removed_length, 0);
        node.delete(&String::from_utf8(vec![b'2'; 24]).unwrap());
        assert_eq!(node.length, 168);
        assert_eq!(node.removed_length, 40);
    }

    #[test]
    fn test_get_works_as_expected_at_big_amounts() {
        let maximum_node_size = 1000;
        let map = BTreeMultiMap::<String, Link, UnsizedNode<MultiPair<String, Link>>>::with_maximum_node_size(maximum_node_size);

        for i in 1..2000 {
            map.insert(
                format!("ValueNum{}", i % 200),
                Link {
                    page_id: Default::default(),
                    offset: i,
                    length: i,
                },
            );
        }

        for i in 1..200 {
            let range = map.get(&format!("ValueNum{i}")).collect::<Vec<_>>();
            assert_eq!(range.len(), 10)
        }
    }
}
