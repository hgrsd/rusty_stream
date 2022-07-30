use std::collections::HashMap;

pub struct LogPositionIndex {
    idx: HashMap<String, Vec<usize>>,
}

impl LogPositionIndex {
    pub fn new() -> Self {
        LogPositionIndex {
            idx: HashMap::new(),
        }
    }

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

    pub fn get_positions(&self, key: &str) -> &[usize] {
        match self.idx.get(key) {
            Some(positions) => &positions,
            None => &[],
        }
    }

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
