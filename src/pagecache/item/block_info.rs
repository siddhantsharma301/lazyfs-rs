#[derive(Debug)]
pub struct BlockInfo {
    readable_offset: (i32, i32),
    pub page_index_number: i32,
}

impl BlockInfo {
    pub fn make_readable_to(&mut self, to: i32) -> i32 {
        if to > self.readable_offset.1 {
            self.readable_offset.1 = to;
        }
        self.readable_offset.1
    }

    pub fn truncate_readable_to(&mut self, to: i32) {
        self.readable_offset.1 = to;
    }

    pub fn clone_readable_offsets(&self) -> (i32, i32) {
        self.readable_offset
    }
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            readable_offset: (0, 0),
            page_index_number: -1,
        }
    }
}
