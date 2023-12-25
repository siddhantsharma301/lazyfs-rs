use std::collections::HashMap;

pub mod backends;
pub mod block_offsets;
pub mod page;

#[derive(Debug, PartialEq)]
enum AllocateOperationType {
    OpRead,       // Specifies that the operation comes from a read operation
    OpWrite,      // Specifies that the operation comes from a write operation
    OpPassthrough, // Specifies the otherwise case (equal to OpRead for now)
}

trait PageCacheEngine {
    fn allocate_blocks(
        &mut self,
        content_owner_id: String,
        block_data_mapping: HashMap<i32, (i32, Vec<u8>, usize, i32)>,
        operation_type: AllocateOperationType,
    ) -> HashMap<i32, i32>;

    fn get_blocks(
        &self,
        content_owner_id: String,
        block_pages: HashMap<i32, (i32, Vec<u8>, i32)>,
    ) -> HashMap<i32, bool>;

    fn is_block_cached(
        &self,
        content_owner_id: String,
        page_id: i32,
        block_id: i32,
    ) -> bool;

    fn make_block_readable_to_offset(
        &mut self,
        cid: String,
        page_id: i32,
        block_id: i32,
        offset: i32,
    );

    fn print_page_cache_engine(&self);

    fn get_engine_usage(&self) -> f64;

    fn remove_cached_blocks(&mut self, content_owner_id: String) -> bool;

    fn sync_pages(&mut self, owner: String, size: u64, orig_path: String) -> bool;

    fn rename_owner_pages(&mut self, old_owner: String, new_owner: String) -> bool;

    fn truncate_cached_blocks(
        &mut self,
        content_owner_id: String,
        blocks_to_remove: HashMap<i32, i32>,
        from_block_id: i32,
        index_inside_block: u64,
    ) -> bool;

    fn get_dirty_blocks_info(&self, owner: String) -> Vec<(i32, (i32, i32), i32)>;
}
