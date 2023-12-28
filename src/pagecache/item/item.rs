use crate::pagecache::item::block_info::BlockInfo;
use crate::pagecache::item::metadata::Metadata;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Item {
    pub data: ItemData,
    pub metadata: Metadata,
    pub is_synced: bool,
}

impl Item {
    pub fn update_metadata(&mut self, new_meta: Metadata, values_to_update: Vec<String>) {
        let old_meta = &mut self.metadata;

        for value in values_to_update.iter() {
            match value.as_str() {
                "size" => old_meta.size = new_meta.size,
                "atime" => old_meta.atim = new_meta.atim,
                "ctime" => old_meta.ctim = new_meta.ctim,
                "mtime" => old_meta.mtim = new_meta.mtim,
                "nlinks" => old_meta.nlinks = new_meta.nlinks,
                _ => (),
            }
        }
    }
}

impl Default for Item {
    fn default() -> Self {
        Self {
            data: ItemData::default(),
            metadata: Metadata::default(),
            is_synced: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ItemData {
    blocks: HashMap<i32, Box<BlockInfo>>,
}

impl ItemData {
    pub fn get_page_id(&self, blk_id: i32) -> i32 {
        match self.blocks.get(&blk_id) {
            Some(block_info) => block_info.page_index_number,
            None => -1,
        }
    }

    pub fn get_readable_offsets(&self, block_id: i32) -> Option<(i32, i32)> {
        match self.blocks.get(&block_id) {
            Some(block_info) => Some(block_info.readable_offset),
            None => None,
        }
    }

    pub fn get_blocks_max_offsets(&self) -> HashMap<i32, i32> {
        self.blocks
            .iter()
            .map(|(&id, block_info)| (id, block_info.clone_readable_offsets().1))
            .collect()
    }

    pub fn truncate_blocks_after(&mut self, blk_id: i32, blk_byte_index: i32) -> HashMap<i32, i32> {
        let mut res = HashMap::new();
        let mut ids_to_remove = Vec::new();

        for (&id, block_info) in self.blocks.iter_mut() {
            if id >= blk_id {
                res.insert(id, block_info.page_index_number);

                if id > blk_id || blk_byte_index == 0 {
                    ids_to_remove.push(id);
                } else if id == blk_id {
                    block_info.truncate_readable_to(blk_byte_index - 1);
                }
            }
        }

        for id in ids_to_remove {
            self.blocks.remove(&id);
        }

        res
    }

    pub fn set_block_page_id(
        &mut self,
        block_id: i32,
        allocated_page: i32,
        readable_from: i32,
        readable_to: i32,
    ) -> i32 {
        let block = self
            .blocks
            .entry(block_id)
            .or_insert_with(|| Box::new(BlockInfo::default()));

        block.page_index_number = allocated_page;
        block.make_readable_to(readable_to)
    }

    pub fn remove_block(&mut self, block_id: i32) {
        self.blocks.remove(&block_id);
    }

    pub fn remove_all(&mut self) {
        self.blocks.clear();
    }

    pub fn has_block(&self, block_id: i32) -> bool {
        self.blocks.contains_key(&block_id)
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}

impl Default for ItemData {
    fn default() -> Self {
        Self {
            blocks: HashMap::with_capacity(30000),
        }
    }
}
