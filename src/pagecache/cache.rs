use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::pagecache::config::Config;
use crate::pagecache::engine::{AllocateOperationType, PageCacheEngine};
use crate::pagecache::item::metadata::Metadata;
use crate::pagecache::item::Item;

pub struct Cache {
    /// Cache configuration struct
    config: Box<Config>,
    /// Maps filenames to the corresponding inodes. If a hard link is created for a file, a new
    /// entry on this map is also created, for the same inode.
    file_inode_mapping: RwLock<HashMap<PathBuf, String>>,
    /// Maps content ids (e.g. file names) to the contents
    contents: RwLock<HashMap<String, Mutex<Item>>>,
    /// Cache engine abstraction struct
    engine: Box<dyn PageCacheEngine>,
}

impl Cache {
    pub fn new(config: Config, engine: impl PageCacheEngine + 'static) -> Self {
        Cache {
            config: Box::new(config),
            file_inode_mapping: RwLock::new(HashMap::with_capacity(1000)),
            contents: RwLock::new(HashMap::with_capacity(1000)),
            engine: Box::new(engine),
        }
    }

    // fn get_content_ptr(&self, cid: String) -> Option<&Mutex<Item>> {
    //     // let lock = self.contents.read().unwrap();
    //     // let mutex = lock.get(&cid);
    //     // mutex
    //     todo!()
    // }

    fn get_readable_offsets(
        &self,
        cid: String,
        item: &MutexGuard<Item>,
        block_id: i32,
    ) -> Option<(i32, i32)> {
        let data = &item.data;
        let page_id = data.get_page_id(block_id);
        if self.engine.is_block_cached(cid, page_id, block_id) {
            return data.get_readable_offsets(block_id);
        }
        None
    }

    pub fn create_item(&mut self, cid: String) -> Result<()> {
        self.contents
            .write()
            .map(|mut l| {
                l.insert(cid.clone(), Mutex::new(Item::default()));
                ()
            })
            .map_err(|e| anyhow!("Failed to create item: {:?}", e))
    }

    pub fn delete_item(&mut self, cid: String) -> Result<()> {
        self.contents
            .write()
            .map(|mut l| {
                l.remove(&cid);
                ()
            })
            .map_err(|e| anyhow!("Failed to create item: {:?}", e))
    }

    pub fn has_content_cached(&self, cid: String) -> Result<bool> {
        match self.contents.read() {
            Ok(lock) => Ok(lock.contains_key(&cid)),
            Err(e) => Err(anyhow!("Failed to read item: {:?}", e)),
        }
    }

    pub fn update_content_metadata(
        &mut self,
        cid: String,
        new_meta: Metadata,
        values_to_update: Vec<String>,
    ) -> Result<bool> {
        let guard = self
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to read item: {:?}", e))?;
        self.update_content_metadata_locked(&guard, cid, new_meta, values_to_update)
    }

    fn update_content_metadata_locked(
        &self,
        contents: &RwLockReadGuard<HashMap<String, Mutex<Item>>>,
        cid: String,
        meta: Metadata,
        values_to_update: Vec<String>,
    ) -> Result<bool> {
        contents.get(&cid).map_or(Ok(false), |item| {
            item.lock()
                .map_err(|e| anyhow!("Failed to lock item: {:?}", e))
                .map(|mut item_lock| {
                    item_lock.update_metadata(meta, values_to_update);
                    true
                })
        })
    }

    pub fn get_content_metadata(&self, cid: String) -> Result<Option<Metadata>> {
        self.contents
            .read()
            .map_err(|e| anyhow!("Failed to read: {:?}", e))
            .and_then(|lock| {
                lock.get(&cid)
                    .map(|item| item.lock())
                    .map_or(Ok(None), |result| {
                        result
                            .map(|item| Some(item.metadata.clone()))
                            .map_err(|e| anyhow!("Failed to read: {:?}", e))
                    })
            })
    }

    pub fn put_data_blocks(
        &mut self,
        cid: String,
        blocks: HashMap<i32, (&Vec<u8>, i32, i32)>,
        operation_type: AllocateOperationType,
    ) -> Result<HashMap<i32, bool>> {
        let is_new = self
            .contents
            .write()
            .map(|l| l.contains_key(&cid.clone()))
            .map_err(|e| anyhow!("Failed to create item: {:?}", e))?;
        if !is_new {
            self.create_item(cid.clone())?;
        }

        let lock = self.contents.read().unwrap();
        let mut item = lock.get(&cid.clone()).unwrap().lock().unwrap();
        let mut put_mapping = HashMap::new();
        for (block_id, (block_data, start, _)) in blocks.clone() {
            let page_id = if is_new {
                -1
            } else {
                item.data.get_page_id(block_id)
            };
            put_mapping.insert(block_id, (page_id, block_data, start));
        }

        let allocations = self
            .engine
            .allocate_blocks(cid.clone(), put_mapping, operation_type)?;
        let mut put_res = HashMap::new();
        let mut allocated_at_least_one_page = false;
        for (block_id, page_id) in allocations {
            let offsets = blocks[&block_id];
            let (_, _, readable_to) = offsets;
            if page_id >= 0 {
                allocated_at_least_one_page = true;
                let max_offset = item
                    .data
                    .set_block_page_id(block_id, page_id, 0, readable_to);
                self.engine.make_block_readable_to_offset(
                    cid.clone(),
                    page_id,
                    block_id,
                    max_offset,
                );
            } else {
                item.data.remove_block(block_id);
            }
            put_res.insert(block_id, page_id >= 0);
        }

        if allocated_at_least_one_page {
            item.is_synced = false;
        }

        Ok(put_res)
    }

    pub fn get_data_blocks(
        &mut self,
        cid: String,
        blocks: HashMap<i32, &[u8]>,
    ) -> Result<HashMap<i32, (bool, Option<(i32, i32)>)>> {
        if !self.has_content_cached(cid.clone())? {
            return Ok(HashMap::new());
        }

        let lock = self.contents.read().unwrap();
        let mut item = lock.get(&cid.clone()).unwrap().lock().unwrap();
        let mut mapping = HashMap::new();
        let max_offset = (self.config.io_block_size - 1) as i32;
        for (block_id, data) in blocks {
            let item_data = &item.data;
            if item_data.has_block(block_id) {
                let old_page = item_data.get_page_id(block_id);
                mapping.insert(block_id, (old_page, data.to_vec(), max_offset));
            }
        }

        let res = self.engine.get_blocks(cid.clone(), mapping)?;
        let mut cache_res = HashMap::new();
        for (block_id, success) in res {
            if !success {
                item.data.remove_block(block_id);
            }
            cache_res.insert(
                block_id,
                (
                    success,
                    self.get_readable_offsets(cid.clone(), &item, block_id),
                ),
            );
        }

        Ok(cache_res)
    }

    pub fn is_block_cached(&self, cid: String, block_id: i32) -> Result<bool> {
        if self.has_content_cached(cid.clone())? {
            let contents = self.contents.read().unwrap();
            if let Some(item) = contents.get(&cid) {
                let item_lock = item.lock().unwrap();
                let page_id = item_lock.data.get_page_id(block_id);
                return Ok(self.engine.is_block_cached(cid, page_id, block_id));
            }
        }
        Ok(false)
    }

    pub fn get_cache_usage(&self) -> f64 {
        self.engine.get_engine_usage()
    }

    pub fn remove_cached_item(
        &mut self,
        owner: String,
        path: PathBuf,
        is_from_cache: bool,
    ) -> Result<bool> {
        if !self.has_content_cached(owner.clone())? {
            return Ok(false);
        }
        let mut lock = self.contents.write().unwrap();
        let mut item = lock.get(&owner.clone()).unwrap().lock().unwrap();
        self.file_inode_mapping.write().unwrap().remove(&path);

        let before_nlinks = item.metadata.nlinks;
        let mut after_meta = item.metadata.clone();
        after_meta.nlinks = std::cmp::max(before_nlinks as u32 - 1, 1);
        item.update_metadata(after_meta, vec!["nlinks".to_string()]);
        if !is_from_cache && before_nlinks > 1 {
            return Ok(false);
        }

        self.engine.remove_cached_blocks(owner.clone());
        drop(item);
        lock.remove(&owner);

        Ok(true)
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

    pub fn clear_cache(&mut self) -> Result<()> {
        let lock = self.file_inode_mapping.read().unwrap();
        let items: Vec<_> = lock.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        // todo: unsure if can drop lock here
        drop(lock);
        for (key, value) in &items {
            self.remove_cached_item(value.to_string(), key.to_path_buf(), true)?;
        }

        let mut lock = self.contents.write().unwrap();
        let items: Vec<_> = lock.keys().cloned().collect();
        for item in items {
            let item_lock = lock.get(&item).unwrap().lock();
            self.engine.remove_cached_blocks(item.clone());
            drop(item_lock);
            lock.remove(&item);
        }

        Ok(())
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

    pub fn get_original_inode(&self, path: PathBuf) -> Result<Option<String>> {
        self.file_inode_mapping
            .read()
            .map_err(|e| anyhow!("Failed to read: {:?}", e))
            .and_then(|lock| Ok(lock.get(&path).cloned()))
    }

    pub fn insert_inode_mapping(
        &mut self,
        path: PathBuf,
        inode: String,
        increase: bool,
    ) -> Result<()> {
        let mut lock = self.file_inode_mapping.write();
        match lock {
            Ok(ref mut lock) => {
                lock.insert(path, inode.clone());
            }
            Err(e) => return Err(anyhow!("Failed to read: {:?}", e)),
        }

        if increase {
            let meta = self.get_content_metadata(inode.clone())?;
            match meta {
                Some(mut meta) => {
                    meta.nlinks += 1;
                    let contents_lock = self.contents.read().unwrap();
                    self.update_content_metadata_locked(
                        &contents_lock,
                        inode,
                        meta,
                        vec!["nlinks".to_string()],
                    )?;
                }
                None => return Err(anyhow!("Unable to fetch metadata of inserted inode!")),
            }
        }

        Ok(())
    }

    pub fn find_files_mapped_to_inode(&self, inode: String) -> Vec<String> {
        todo!()
    }
}
