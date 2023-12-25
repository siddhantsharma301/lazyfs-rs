use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct BlockOffsets {
    block_offset_mapping: HashMap<i32, (i32, i32)>,
    block_readable_to: HashMap<i32, i32>,
}

impl BlockOffsets {
    pub fn reset(&mut self) {
        self.block_offset_mapping.clear();
        self.block_readable_to.clear();
    }

    pub fn contains_block(&self, block_id: i32) -> bool {
        self.block_offset_mapping.contains_key(&block_id)
    }

    pub fn get_block_offsets(&self, block_id: i32) -> (i32, i32) {
        *self.block_offset_mapping.get(&block_id).unwrap_or(&(-1, -1))
    }

    fn get_nr_blocks(&self) -> usize {
        self.block_offset_mapping.len()
    }

    pub fn insert_or_update_block_offsets(&mut self, block_id: i32, offsets: (i32, i32)) {
        self.block_offset_mapping.insert(block_id, offsets);
    }

    pub fn make_readable_to(&mut self, blk_id: i32, max_offset: i32) {
        self.block_readable_to.insert(blk_id, max_offset);
    }

    pub fn get_block_readable_offsets(&self) -> HashMap<i32, i32> {
        self.block_readable_to.clone()
    }

    pub fn with_capacity(&mut self, capacity: usize) {
        self.block_offset_mapping.reserve(capacity);
        self.block_readable_to.reserve(capacity);
    }

    pub fn get_readable_to(&self, block_id: i32) -> i32 {
        *self.block_readable_to.get(&block_id).unwrap_or(&0)
    }

    pub fn remove_block(&mut self, blk_id: i32) {
        self.block_offset_mapping.remove(&blk_id);
        self.block_readable_to.remove(&blk_id);
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
