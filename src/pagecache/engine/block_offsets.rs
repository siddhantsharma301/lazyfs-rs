use std::collections::HashMap;

use crate::pagecache::{BlockId, Offsets};

#[derive(Clone, Debug)]
pub struct BlockOffsets {
    block_offset_mapping: HashMap<BlockId, Offsets>,
    block_readable_to: HashMap<BlockId, i32>,
}

impl BlockOffsets {
    pub fn reset(&mut self) {
        self.block_offset_mapping.clear();
        self.block_readable_to.clear();
    }

    pub fn contains_block(&self, block_id: BlockId) -> bool {
        self.block_offset_mapping.contains_key(&block_id)
    }

    pub fn get_block_offsets(&self, block_id: BlockId) -> Offsets {
        *self.block_offset_mapping.get(&block_id).unwrap_or(&(-1, -1))
    }

    fn get_nr_blocks(&self) -> usize {
        self.block_offset_mapping.len()
    }

    pub fn insert_or_update_block_offsets(&mut self, block_id: BlockId, offsets: Offsets) {
        self.block_offset_mapping.insert(block_id, offsets);
    }

    pub fn make_readable_to(&mut self, block_id: BlockId, max_offset: i32) {
        self.block_readable_to.insert(block_id, max_offset);
    }

    pub fn get_block_readable_offsets(&self) -> HashMap<i32, i32> {
        self.block_readable_to.clone()
    }

    pub fn with_capacity(&mut self, capacity: usize) {
        self.block_offset_mapping.reserve(capacity);
        self.block_readable_to.reserve(capacity);
    }

    pub fn get_readable_to(&self, block_id: BlockId) -> i32 {
        *self.block_readable_to.get(&block_id).unwrap_or(&0)
    }

    pub fn remove_block(&mut self, block_id: BlockId) {
        self.block_offset_mapping.remove(&block_id);
        self.block_readable_to.remove(&block_id);
    }

    pub fn empty(&self) -> bool {
        self.block_offset_mapping.is_empty()
    }
}

impl Default for BlockOffsets {
    fn default() -> Self {
        Self {
            block_offset_mapping: HashMap::new(),
            block_readable_to: HashMap::new(),
        }
    }
}
