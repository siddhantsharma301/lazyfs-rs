use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use crate::pagecache::config::Config;
use crate::pagecache::engine::{AllocateOperationType, PageCacheEngine};
use crate::pagecache::item::metadata::Metadata;
use crate::pagecache::item::Item;

pub struct Cache {
    /// A cache configuration object.
    /// Assuming `CacheConfig` is a Rust equivalent of `cache::config::Config`.
    config: Box<Config>,
    inner: RwLock<CacheInner>
}

struct CacheInner {
    /// Maps filenames to the corresponding inodes.
    /// If a hard link is created for a file, a new entry
    /// on this map is also created, for the same inode.
    file_inode_mapping: HashMap<String, String>,

    /// Maps content ids (e.g., file names) to the contents.
    contents: HashMap<String, Box<Item>>,

    /// Maps each content to a lock mutex.
    item_locks: HashMap<String, RwLock<Item>>,

    /// Cache engine abstraction object.
    /// Assuming `PageCacheEngine` is already defined in Rust.
    engine: Box<dyn PageCacheEngine>,
}

impl CacheInner {
    fn new(engine: impl PageCacheEngine + 'static) -> Self {
        CacheInner {
            file_inode_mapping: HashMap::with_capacity(1000),
            contents: HashMap::new(),
            item_locks: HashMap::with_capacity(1000),
            engine: Box::new(engine)
        }
    }
}

impl Cache {
    pub fn new(config: Config, engine: impl PageCacheEngine + 'static) -> Self {
        Cache {
            config: Box::new(config),
            inner: RwLock::new(CacheInner::new(engine))
        }
    }

    fn get_content_ptr(&self, cid: String) -> Item {
        todo!()
    }

    fn get_readable_offsets(&self, cid: String, item: Item, blk: i32) -> (i32, i32) {
        todo!()
    }

    pub fn create_item(&mut self, cid: String) -> Item {
        todo!()
    }

    pub fn delete_item(&mut self, cid: String) -> Result<()> {
        todo!()
    }

    pub fn has_content_cached(&self, cid: String) -> bool {
        todo!()
    }

    pub fn update_content_metadata(
        &mut self,
        cid: String,
        new_meta: Metadata,
        values_to_update: Vec<String>,
    ) -> Result<()> {
        todo!()
    }

    pub fn get_content_metadata(&self, cid: String) -> Metadata {
        todo!()
    }

    pub fn put_data_blocks(
        cid: String,
        blocks: HashMap<i32, (Vec<u8>, usize, i32, i32)>,
        operation_type: AllocateOperationType,
    ) -> HashMap<i32, bool> {
        todo!()
    }

    pub fn get_data_blocks(
        cid: String,
        blocks: HashMap<i32, &str>,
    ) -> HashMap<i32, (bool, (i32, i32))> {
        todo!()
    }

    pub fn is_block_cached(&self, cid: String, blk_id: i32) -> bool {
        todo!()
    }

    pub fn get_cache_usage(&self) -> f64 {
        todo!()
    }

    pub fn remove_cached_item(
        &mut self,
        owner: String,
        path: PathBuf,
        is_from_cache: bool,
    ) -> Result<()> {
        todo!()
    }

    pub fn sync_owner(
        &mut self,
        owner: String,
        only_sync_data: bool,
        orig_path: PathBuf,
    ) -> Result<()> {
        todo!()
    }

    pub fn rename_item(&mut self, old_cid: String, new_cid: String) -> Result<()> {
        todo!()
    }

    pub fn clear_cache(&mut self) {
        todo!()
    }

    pub fn truncate_item(&mut self, owner: String, new_size: usize) -> Result<()> {
        todo!()
    }

    pub fn full_checkpoint(&mut self) {
        todo!()
    }

    pub fn report_unsynced_data(&self) -> Vec<(String, usize, Vec<(i32, (i32, i32), i32)>)> {
        todo!()
    }

    pub fn get_original_inode(&self, path: PathBuf) -> &str {
        todo!()
    }

    pub fn insert_inode_mapping(&mut self, path: PathBuf, inode: String, increase: bool) {
        todo!()
    }

    pub fn find_files_mapped_to_inode(&self, inode: String) -> Vec<String> {
        todo!()
    }
}
