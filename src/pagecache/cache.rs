use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

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

    fn get_content_ptr(&self, cid: String) -> Option<&Mutex<Item>> {
        // let lock = self.contents.read().unwrap();
        // let mutex = lock.get(&cid);
        // mutex
        todo!()
    }

    fn get_readable_offsets(&self, cid: String, item: Item, blk: i32) -> (i32, i32) {
        todo!()
    }

    pub fn create_item(&mut self, cid: String) -> Result<()> {
        let item = Mutex::new(Item::default());
        let lock = self.contents.write();
        match lock {
            Ok(mut l) => {
                l.insert(cid.clone(), item);
                Ok(())
            }
            Err(e) => Err(anyhow!("Failed to create item: {:?}", e)),
        }
    }

    pub fn delete_item(&mut self, cid: String) -> Result<()> {
        todo!()
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
        self.contents
            .read()
            .map_err(|e| anyhow!("Failed to read item: {:?}", e))
            .and_then(|lock| {
                lock.get(&cid).map_or(Ok(false), |item| {
                    item.lock()
                        .map_err(|e| anyhow!("Failed to lock item: {:?}", e))
                        .map(|mut item_lock| {
                            item_lock.update_metadata(new_meta, values_to_update);
                            true
                        })
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
        drop(lock);

        if increase {
            let meta = self.get_content_metadata(inode.clone())?;
            match meta {
                Some(mut meta) => {
                    meta.nlinks += 1;
                    self.update_content_metadata(inode, meta, vec!["nlinks".to_string()])?;
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
