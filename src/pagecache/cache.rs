use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::{FileTimes, OpenOptions};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use crate::pagecache::config::Config;
use crate::pagecache::engine::{AllocateOperationType, PageCacheEngine};
use crate::pagecache::item::metadata::Metadata;
use crate::pagecache::item::Item;

pub struct Cache {
    /// Cache configuration struct
    config: Box<Config>,
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    /// Maps filenames to the corresponding inodes. If a hard link is created for a file, a new
    /// entry on this map is also created, for the same inode.
    file_inode_mapping: RwLock<HashMap<PathBuf, String>>,
    /// Maps content ids (e.g. file names) to the contents
    contents: RwLock<HashMap<String, Mutex<Item>>>,
    /// Cache engine abstraction struct
    engine: RwLock<Box<dyn PageCacheEngine>>,
}

impl CacheInner {
    fn new(engine: impl PageCacheEngine + 'static) -> Self {
        Self {
            contents: RwLock::new(HashMap::new()),
            file_inode_mapping: RwLock::new(HashMap::new()),
            engine: RwLock::new(Box::new(engine)),
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
    ) -> Result<Option<(i32, i32)>> {
        let data = &item.data;
        let page_id = data.get_page_id(block_id);

        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let engine = inner
            .engine
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        if engine.is_block_cached(cid, page_id, block_id) {
            return Ok(data.get_readable_offsets(block_id));
        }

        Ok(None)
    }

    pub fn insert_item(&self, cid: String) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock oncontents: {:?}", e))?;

        contents.insert(cid, Mutex::new(Item::default()));
        Ok(())
    }

    pub fn insert_item_if_not_exists(&self, cid: String) -> Result<bool> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let is_new = contents.contains_key(&cid.clone());
        if !is_new {
            contents.insert(cid.clone(), Mutex::new(Item::default()));
        }
        Ok(is_new)
    }

    pub fn remove_item(&self, cid: String) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on contents: {:?}", e))?;

        contents.remove(&cid);
        Ok(())
    }

    pub fn has_content_cached(&self, cid: String) -> Result<bool> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;

        Ok(contents.contains_key(&cid))
    }
    pub fn update_content_metadata(
        &self,
        cid: String,
        metadata: Metadata,
        values_to_update: Vec<String>,
    ) -> Result<bool> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        self.update_content_metadata_inner(&inner, cid, metadata, values_to_update)
    }

    fn update_content_metadata_inner(
        &self,
        inner: &RwLockWriteGuard<CacheInner>,
        cid: String,
        metadata: Metadata,
        values_to_update: Vec<String>,
    ) -> Result<bool> {
        let contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on contents: {:?}", e))?;

        match contents.get(&cid) {
            Some(item) => {
                let mut item = item
                    .lock()
                    .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;
                item.update_metadata(metadata, values_to_update);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn get_content_metadata(&self, cid: String) -> Result<Option<Metadata>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;

        match contents.get(&cid) {
            Some(item) => {
                let item = item
                    .lock()
                    .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;
                Ok(Some(item.metadata.clone()))
            }
            None => Ok(None),
        }
    }

    pub fn put_data_blocks(
        &mut self,
        cid: String,
        blocks: HashMap<i32, (&Vec<u8>, i32, i32)>,
        operation_type: AllocateOperationType,
    ) -> Result<HashMap<i32, bool>> {
        let is_new = self.insert_item_if_not_exists(cid.clone())?;

        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let mut item = contents
            .get(&cid.clone())
            .unwrap()
            .lock()
            .map_err(|e| anyhow!("Failed to acquire read lock on items: {:?}", e))?;

        let mut put_mapping = HashMap::new();
        for (block_id, (block_data, start, _)) in blocks.clone() {
            let page_id = if is_new {
                -1
            } else {
                item.data.get_page_id(block_id)
            };
            put_mapping.insert(block_id, (page_id, block_data, start));
        }

        let mut engine = inner
            .engine
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        let allocations = engine.allocate_blocks(cid.clone(), put_mapping, operation_type)?;
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
                engine.make_block_readable_to_offset(cid.clone(), page_id, block_id, max_offset);
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

        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let mut item = contents.get(&cid.clone()).unwrap().lock().unwrap();

        let mut mapping = HashMap::new();
        let max_offset = (self.config.io_block_size - 1) as i32;
        for (block_id, data) in blocks {
            let item_data = &item.data;
            if item_data.has_block(block_id) {
                let old_page = item_data.get_page_id(block_id);
                mapping.insert(block_id, (old_page, data.to_vec(), max_offset));
            }
        }

        let mut engine = inner
            .engine
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        let res = engine.get_blocks(cid.clone(), mapping)?;
        let mut cache_res = HashMap::new();
        for (block_id, success) in res {
            if !success {
                item.data.remove_block(block_id);
            }
            cache_res.insert(
                block_id,
                (
                    success,
                    self.get_readable_offsets(cid.clone(), &item, block_id)?,
                ),
            );
        }

        Ok(cache_res)
    }

    pub fn is_block_cached(&self, cid: String, block_id: i32) -> Result<bool> {
        if !self.has_content_cached(cid.clone())? {
            return Ok(false);
        }

        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;

        if let Some(item) = contents.get(&cid) {
            let item_lock = item
                .lock()
                .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;

            let page_id = item_lock.data.get_page_id(block_id);
            let engine = inner
                .engine
                .read()
                .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
            return Ok(engine.is_block_cached(cid, page_id, block_id));
        }

        Ok(false)
    }

    pub fn get_cache_usage(&self) -> Result<f64> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let engine = inner
            .engine
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        Ok(engine.get_engine_usage())
    }

    pub fn remove_cached_item(
        &self,
        owner: String,
        path: PathBuf,
        is_from_cache: bool,
    ) -> Result<bool> {
        if !self.has_content_cached(owner.clone())? {
            return Ok(false);
        }

        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;

        self.remove_cached_item_inner(&inner, owner.clone(), path, is_from_cache)?;

        let mut engine = inner
            .engine
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        engine.remove_cached_blocks(owner);

        Ok(true)
    }

    fn remove_cached_item_inner(
        &self,
        inner: &RwLockWriteGuard<CacheInner>,
        owner: String,
        path: PathBuf,
        is_from_cache: bool,
    ) -> Result<bool> {
        let mut file_inode_mapping = inner
            .file_inode_mapping
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on file inode mapping: {:?}", e))?;
        file_inode_mapping.remove(&path);

        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let mut item = contents.get(&owner.clone()).unwrap().lock().unwrap();

        let before_nlinks = item.metadata.nlinks;
        let mut after_meta = item.metadata.clone();
        after_meta.nlinks = std::cmp::max(before_nlinks as u32 - 1, 1);
        item.update_metadata(after_meta, vec!["nlinks".to_string()]);
        if !is_from_cache && before_nlinks > 1 {
            return Ok(false);
        }
        drop(item);
        contents.remove(&owner);

        Ok(true)
    }

    pub fn sync_owner(
        &self,
        owner: String,
        only_sync_data: bool,
        orig_path: PathBuf,
    ) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on inner: {:?}", e))?;

        if !self.has_content_cached(owner.clone())? {
            return Err(anyhow!("Content not cached"));
        }

        self.sync_owner_inner(&inner, owner, only_sync_data, orig_path)
    }

    fn sync_owner_inner(
        &self,
        inner: &RwLockWriteGuard<CacheInner>,
        owner: String,
        only_sync_data: bool,
        orig_path: PathBuf,
    ) -> Result<()> {
        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to read contents: {:?}", e))?;
        let item = contents
            .get(&owner)
            .ok_or_else(|| anyhow!("Item not found"))?
            .lock()
            .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;
        let last_size = item.metadata.size;

        let mut engine = inner
            .engine
            .write()
            .map_err(|e| anyhow!("Failed to read engine: {:?}", e))?;
        engine.sync_pages(
            owner.clone(),
            last_size,
            orig_path.to_string_lossy().to_string(),
        )?;

        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to read contents: {:?}", e))?;
        let mut item = contents
            .get_mut(&owner)
            .ok_or_else(|| anyhow!("Item not found"))?
            .lock()
            .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;
        item.is_synced = true;

        if !only_sync_data {
            let meta = &item.metadata;
            let file_times = FileTimes::new();
            file_times.set_accessed(meta.atim);
            file_times.set_modified(meta.mtim);
            let fd = OpenOptions::new().write(true).open(orig_path)?;
            fd.set_times(file_times)?;
        }

        Ok(())
    }

    pub fn rename_item(&mut self, old_cid: PathBuf, new_cid: PathBuf) -> Result<bool> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on 'inner': {:?}", e))?;

        if let Some(inode) = inner
            .file_inode_mapping
            .write()
            .map_err(|e| {
                anyhow!(
                    "Failed to acquire write lock on 'file_inode_mapping': {:?}",
                    e
                )
            })?
            .get(&old_cid)
            .cloned()
        {
            let mut file_inode_mapping = inner.file_inode_mapping.write().map_err(|e| {
                anyhow!(
                    "Failed to acquire write lock on 'file_inode_mapping': {:?}",
                    e
                )
            })?;

            let to_remove_inode = file_inode_mapping
                .remove(&new_cid)
                .unwrap_or_else(|| "".to_string());
            file_inode_mapping.insert(new_cid, inode.clone());

            if let Some(item_mutex) = inner
                .contents
                .write()
                .map_err(|e| anyhow!("Failed to acquire write lock on content': {:?}", e))?
                .get(&to_remove_inode)
            {
                let mut item = item_mutex
                    .lock()
                    .map_err(|e| anyhow!("Failed to lock item: {:?}", e))?;

                let mut metadata = item.metadata.clone();
                let before_nlinks = metadata.nlinks;
                let new_nlinks = std::cmp::max(before_nlinks - 1, 1);
                metadata.nlinks = new_nlinks;
                item.update_metadata(metadata, vec!["nlinks".to_string()]);

                if before_nlinks <= 1 {
                    inner
                        .engine
                        .write()
                        .map_err(|e| anyhow!("Failed to acquire write lock on engine: {:?}", e))?
                        .remove_cached_blocks(to_remove_inode.clone());

                    inner
                        .contents
                        .write()
                        .map_err(|e| anyhow!("Failed to acquire write lock on content': {:?}", e))?
                        .remove(&to_remove_inode);
                }
            }
        }

        Ok(true)
    }

    pub fn clear_cache(&mut self) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let file_inode_mapping = inner
            .file_inode_mapping
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on file inode mapping: {:?}", e))?;
        let items: Vec<_> = file_inode_mapping
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (key, value) in &items {
            self.remove_cached_item_inner(&inner, value.to_string(), key.to_path_buf(), true)?;
        }

        let mut contents = inner
            .contents
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let mut engine = inner
            .engine
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;
        let items: Vec<_> = contents.keys().cloned().collect();
        for item in items {
            engine.remove_cached_blocks(item.clone());
            contents.remove(&item);
        }

        Ok(())
    }

    //     pub fn truncate_item(&mut self, owner: String, new_size: usize) -> Result<()> {
    //         todo!()
    //     }

    pub fn full_checkpoint(&self) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on inner: {:?}", e))?;
        let file_inode_mapping = inner.file_inode_mapping.read().map_err(|e| {
            anyhow!(
                "Failed to acquire write lock on file inode mapping: {:?}",
                e
            )
        })?;

        for (path, owner) in file_inode_mapping.iter() {
            self.sync_owner_inner(&inner, owner.clone(), false, path.clone())?;
        }
        Ok(())
    }

    pub fn report_unsynced_data(
        &self,
    ) -> Result<Vec<(String, usize, Vec<(i32, (i32, i32), i32)>)>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let contents = inner
            .contents
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on contents: {:?}", e))?;
        let engine = inner
            .engine
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on engine: {:?}", e))?;

        let mut unsynced = Vec::new();
        for (owner, item) in contents.iter() {
            let item = item
                .lock()
                .map_err(|e| anyhow!("Failed to acquire read lock on item: {:?}", e))?;
            if !item.is_synced {
                unsynced.push((
                    owner.clone(),
                    0usize,
                    engine.get_dirty_blocks_info(owner.to_string()),
                ));
            }
        }

        Ok(unsynced)
    }

    pub fn get_original_inode(&self, path: PathBuf) -> Result<Option<String>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let file_inode_mapping = inner
            .file_inode_mapping
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on file inode mapping: {:?}", e))?;
        Ok(file_inode_mapping.get(&path).cloned())
    }

    pub fn insert_inode_mapping(
        &mut self,
        path: PathBuf,
        inode: String,
        increase: bool,
    ) -> Result<()> {
        let inner = self
            .inner
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let mut file_inode_mapping = inner
            .file_inode_mapping
            .write()
            .map_err(|e| anyhow!("Failed to acquire read lock on file inode mapping: {:?}", e))?;
        file_inode_mapping.insert(path, inode.clone());

        if increase {
            let metadata = self.get_content_metadata(inode.clone())?;
            match metadata {
                Some(mut metadata) => {
                    metadata.nlinks += 1;
                    self.update_content_metadata_inner(
                        &inner,
                        inode,
                        metadata,
                        vec!["nlinks".to_string()],
                    )?;
                }
                None => return Err(anyhow!("Unable to fetch metadata of inserted inode!")),
            }
        }

        Ok(())
    }

    pub fn find_files_mapped_to_inode(&self, inode: String) -> Result<Vec<PathBuf>> {
        let inner = self
            .inner
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {:?}", e))?;
        let file_inode_mapping = inner
            .file_inode_mapping
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on file inode mapping: {:?}", e))?;
        Ok(file_inode_mapping
            .iter()
            .filter(|(_key, val)| **val == inode)
            .map(|(key, _val)| key.clone())
            .collect::<Vec<_>>())
    }
}
