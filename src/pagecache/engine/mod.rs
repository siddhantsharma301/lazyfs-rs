use anyhow::Result;
use std::collections::HashMap;

pub mod backends;
pub mod block_offsets;
pub mod page;

#[derive(Debug, PartialEq)]
pub enum AllocateOperationType {
    OpRead,       // Specifies that the operation comes from a read operation
    OpWrite,      // Specifies that the operation comes from a write operation
    OpPassthrough, // Specifies the otherwise case (equal to OpRead for now)
}

pub trait PageCacheEngine {
    fn allocate_blocks(
        &self,
        content_owner_id: String,
        block_data_mapping: HashMap<i32, (i32, &Vec<u8>, i32)>,
        operation_type: AllocateOperationType,
    ) -> Result<HashMap<i32, i32>>;

    fn get_blocks(
        &self,
        content_owner_id: String,
        block_pages: HashMap<i32, (i32, Vec<u8>, i32)>,
    ) -> Result<HashMap<i32, bool>>;

    fn is_block_cached(
        &self,
        content_owner_id: String,
        page_id: i32,
        block_id: i32,
    ) -> Result<bool>;

    fn make_block_readable_to_offset(
        &self,
        cid: String,
        page_id: i32,
        block_id: i32,
        offset: i32,
    ) -> Result<()>;

    fn get_engine_usage(&self) -> Result<f64>;

    fn remove_cached_blocks(&self, content_owner_id: String) -> Result<bool>;

    fn sync_pages(&self, owner: String, size: u32, orig_path: String) -> Result<()>;

    fn rename_owner_pages(&self, old_owner: String, new_owner: String) -> Result<bool>;

    fn truncate_cached_blocks(
        &self,
        content_owner_id: String,
        blocks_to_remove: HashMap<i32, i32>,
        from_block_id: i32,
        index_inside_block: i32,
    ) -> Result<bool>;

    fn get_dirty_blocks_info(&self, owner: String) -> Result<Vec<(i32, (i32, i32), i32)>>;
}
