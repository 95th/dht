use std::collections::HashMap;

pub struct IndexMap<T> {
    map: HashMap<usize, T>,
    next_id: usize,
}

impl<T> Default for IndexMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IndexMap<T> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        self.map.insert(id, value);
        id
    }

    pub fn remove(&mut self, key: usize) -> Option<T> {
        self.map.remove(&key)
    }

    pub fn get(&self, key: usize) -> Option<&T> {
        self.map.get(&key)
    }

    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        self.map.get_mut(&key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
