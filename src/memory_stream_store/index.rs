use std::collections::HashMap;

/// An index that can retrieve and store global positions in the message log for a given key.
/// Indices are append-only.
/// 
/// An index is implemented as a HashMap where each key is the name of the relevant index, and each
/// value is a vector of pointers (indices) into the global log.
pub struct LogPositionIndex {
    idx: HashMap<String, Vec<usize>>,
}

impl LogPositionIndex {
    pub fn new() -> Self {
        LogPositionIndex {
            idx: HashMap::new(),
        }
    }

    /// Append a new log position to the index for a given key
    pub fn write_position(&mut self, key: &str, position: usize) {
        match self.idx.get_mut(key) {
            Some(position_list) => {
                position_list.push(position);
            }
            None => {
                self.idx.insert(key.to_owned(), vec![position]);
            }
        }
    }

    /// Get log positions for a given key
    pub fn get_positions(&self, key: &str) -> &[usize] {
        match self.idx.get(key) {
            Some(positions) => positions,
            None => &[],
        }
    }

    /// Get log positions for a given key after a certain offset. 
    ///
    /// This is a more expensive operation than getting all positions for an index. Its worst case
    /// runtime is O(log n) where n is the total number of positions in the index for the given
    /// key.
    pub fn get_positions_after(&self, key: &str, offset: usize) -> &[usize] {
        match self.idx.get(key) {
            Some(positions) => {
                let start = if offset == 0 {
                    0
                } else {
                    match positions.binary_search(&offset) {
                        Ok(x) => x,
                        Err(x) => x,
                    }
                };
                &positions[start..]
            }
            None => &[],
        }
    }
}
