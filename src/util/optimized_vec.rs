/// Struct for storing data in a vector.
/// It has a vector of data and vector of empty indexes.
/// If the `empty` vector is empty, then the data vector is extended.
/// If the `empty` vector is not empty, then index from the empty vector is used to insert the data.
#[derive(Debug)]
pub struct OptimizedVec<T> {
    /// Vector of data.
    data: Vec<T>,
    /// Vector of empty indexes.
    empty: Vec<usize>,
    /// Flag to check if the item is empty.
    emptiness: Vec<bool>,
    /// Number of elements in the vector.
    length: usize,
}

impl<T> Default for OptimizedVec<T> {
    fn default() -> Self {
        OptimizedVec {
            data: Vec::new(),
            empty: Vec::new(),
            emptiness: Vec::new(),
            length: 0,
        }
    }
}

impl<T> OptimizedVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        OptimizedVec {
            data: Vec::with_capacity(cap),
            empty: Vec::with_capacity(cap),
            emptiness: Vec::with_capacity(cap),
            length: 0,
        }
    }

    /// Pushes a value to the vector.
    /// # Arguments
    /// * `value` - Value to push
    /// # Returns
    /// * `usize` - Index of the pushed value
    pub fn push(&mut self, value: T) -> usize {
        let index = if self.empty.is_empty() {
            self.data.push(value);
            self.emptiness.push(false);
            self.length
        } else {
            let index = self.empty.pop().unwrap();
            self.data[index] = value;
            self.emptiness[index] = false;
            index
        };

        self.length += 1;

        index
    }

    /// Gets a value from the vector.
    /// # Arguments
    /// * `index` - Index of the value to get
    /// # Returns
    /// * `Option<T>` - Value at the index,
    ///   or `None` if the index is out of bounds or the value is empty.
    #[allow(dead_code)]
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.data.len() {
            return None;
        }

        if self.emptiness[index] {
            return None;
        }

        Some(&self.data[index])
    }

    /// Gets a mutable value from the vector.
    /// # Arguments
    /// * `index` - Index of the value to get
    /// # Returns
    /// * `Option<&mut T>` - Mutable value at the index,
    ///   or `None` if the index is out of bounds or the value is empty.
    #[allow(dead_code)]
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        if index >= self.data.len() {
            return None;
        }

        if self.emptiness[index] {
            return None;
        }

        Some(&mut self.data[index])
    }

    /// Removes a value from the vector.
    /// # Arguments
    /// * `index` - Index of the value to remove.
    /// # Returns
    /// * `Option<T>` - Value at the index,
    ///   or `None` if the index is out of bounds or the value is empty.
    pub fn remove(&mut self, index: usize) -> Option<T>
    where
        T: Clone,
    {
        if index >= self.data.len() {
            return None;
        }

        if self.emptiness[index] {
            return None;
        }

        self.emptiness[index] = true;
        self.empty.push(index);
        self.length -= 1;

        Some(self.data[index].clone())
    }

    /// Gets the data vector.
    /// # Returns
    /// * `&Vec<T>` - Data vector.
    #[allow(dead_code)]
    pub fn get_data(&self) -> &Vec<T> {
        &self.data
    }

    /// Gets the length of the vector.
    /// # Returns
    /// * `usize` - Length of the vector.
    #[allow(dead_code)]
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Returns true of [`OptimizedVec`] is empty.
    /// # Returns
    /// * `bool` - State of emptiness.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::OptimizedVec;

    #[test]
    fn test_optimized_vec_new() {
        let vec = OptimizedVec::<i32>::default();

        assert_eq!(vec.data.len(), 0);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.emptiness.len(), 0);

        assert_eq!(vec.length, 0);
    }

    #[test]
    fn test_optimized_vec_push() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.emptiness.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);
    }

    #[test]
    fn test_optimized_vec_get() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.get(index), Some(&1));
        assert_eq!(vec.get(index + 1), None);
    }

    #[test]
    fn test_optimized_vec_get_mut() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.get_mut(index), Some(&mut 1));
        assert_eq!(vec.get_mut(index + 1), None);
    }

    #[test]
    fn test_optimized_vec_remove() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(vec.remove(index), Some(1));
        assert_eq!(vec.remove(index + 1), None);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.emptiness.len(), 1);
        assert_eq!(vec.empty.len(), 1);
        assert_eq!(vec.empty[0], index);
        assert_eq!(vec.length, 0);
    }

    #[test]
    fn test_optimized_vec_push_remove() {
        let mut vec = OptimizedVec::<i32>::default();
        let index = vec.push(1);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.emptiness.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);

        assert_eq!(vec.remove(index), Some(1));

        let index = vec.push(2);

        assert_eq!(index, 0);
        assert_eq!(vec.data.len(), 1);
        assert_eq!(vec.emptiness.len(), 1);
        assert_eq!(vec.empty.len(), 0);
        assert_eq!(vec.length, 1);
    }
}
