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
    config: Box<Config>,
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    /// Maps filenames to the corresponding inodes.
    /// If a hard link is created for a file, a new entry
    /// on this map is also created, for the same inode.
    file_inode_mapping: HashMap<PathBuf, String>,

    /// Maps content ids (e.g., file names) to the contents.
    contents: HashMap<String, RwLock<Item>>,

    /// Cache engine abstraction object.
    engine: Box<dyn PageCacheEngine>,
}

impl CacheInner {
    fn new(engine: impl PageCacheEngine + 'static) -> Self {
        CacheInner {
            file_inode_mapping: HashMap::with_capacity(1000),
            contents: HashMap::with_capacity(1000),
            engine: Box::new(engine),
        }
    }
}

impl Cache {
    pub fn new(config: Config, engine: impl PageCacheEngine + 'static) -> Self {
        Cache {
            config: Box::new(config),
            inner: RwLock::new(CacheInner::new(engine)),
        }
    }

    fn get_content_ptr(&self, cid: String) -> Option<&RwLock<Item>> {
        let lock = self.inner.read().unwrap();
        lock.contents.get(&cid)
    }

    fn get_readable_offsets(&self, cid: String, item: Item, blk: i32) -> (i32, i32) {
        todo!()
    }

    pub fn create_item(&mut self, cid: String) -> &RwLock<Item> {
        let item = RwLock::new(Item::default());
        let mut lock = self.inner.write().unwrap();
        lock.contents.insert(cid.clone(), item);
        lock.contents.get(&cid).unwrap()
    }

    pub fn delete_item(&mut self, cid: String) -> Result<()> {
        todo!()
    }

    pub fn has_content_cached(&self, cid: String) -> bool {
        self.inner.read().unwrap().contents.contains_key(&cid)
    }

    pub fn update_content_metadata(
        &mut self,
        cid: String,
        new_meta: Metadata,
        values_to_update: Vec<String>,
    ) -> bool {
        let lock = self.inner.read().unwrap();
        if let Some(item) = lock.contents.get(&cid) {
            let mut item_lock = item.write().unwrap();
            item_lock.update_metadata(new_meta, values_to_update);
            true
        } else {
            false
        }
    }

    pub fn get_content_metadata(&self, cid: String) -> Option<Metadata> {
        let item = self.get_content_ptr(cid);
        match item {
            Some(item) => {
                let lock = item.read().unwrap();
                Some(lock.metadata.clone())
            }
            None => None,
        }
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

    pub fn get_original_inode(&self, path: PathBuf) -> Option<String> {
        let lock = self.inner.read().unwrap();
        lock.file_inode_mapping.get(&path).cloned()
    }

    pub fn insert_inode_mapping(&mut self, path: PathBuf, inode: String, increase: bool) {
        let mut meta = None;
        if increase {
            let _lock = self.inner.read().unwrap();
            if let Some(mut m) = self.get_content_metadata(inode.clone()) {
                m.nlinks += 1;
                meta = Some(m);
            }
        }

        match meta {
            Some(meta) => {
                let mut lock = self.inner.write().unwrap();
                lock.file_inode_mapping.insert(path, inode.clone());
                drop(lock);
                self.update_content_metadata(inode, meta.clone(), vec!["nlinks".to_string()]);
            }
            None => {}
        }
    }

    pub fn find_files_mapped_to_inode(&self, inode: String) -> Vec<String> {
        todo!()
    }
}
