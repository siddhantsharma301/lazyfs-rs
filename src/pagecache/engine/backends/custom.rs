use crate::pagecache::config::Config;
use crate::pagecache::engine::page::Page;
use crate::pagecache::engine::{AllocateOperationType, PageCacheEngine};
use crate::pagecache::{BlockId, Offsets, PageId};
use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type PageSynced = bool;

#[derive(Debug)]
pub struct CustomCacheEngine {
    config: Box<Config>,
    data: RwLock<CustomCacheEngineInner>,
}

#[derive(Debug)]
pub(crate) struct CustomCacheEngineInner {
    search_index: HashMap<i32, Box<Page>>,
    free_pages: Vec<i32>,
    owner_pages_mapping: HashMap<String, HashSet<i32>>,
    owner_ordered_pages_mapping:
        HashMap<String, HashMap<BlockId, (PageId, Box<Page>, Offsets, PageSynced)>>,
    owner_free_pages_mapping: HashMap<String, Vec<i32>>,

    lru_main_vector: VecDeque<i32>,
    page_order_mapping: HashMap<i32, i32>,
}

impl CustomCacheEngineInner {
    pub fn new() -> Self {
        CustomCacheEngineInner {
            search_index: HashMap::new(),
            free_pages: Vec::new(),
            owner_pages_mapping: HashMap::new(),
            owner_ordered_pages_mapping: HashMap::new(),
            owner_free_pages_mapping: HashMap::new(),

            lru_main_vector: VecDeque::new(),
            page_order_mapping: HashMap::new(),
        }
    }
}

impl CustomCacheEngine {
    pub fn new(config: Box<Config>) -> Self {
        CustomCacheEngine {
            config,
            data: RwLock::new(CustomCacheEngineInner::new()),
        }
    }

    fn get_page_ptr_read(
        &self,
        data: &RwLockReadGuard<CustomCacheEngineInner>,
        page_id: PageId,
    ) -> Option<Box<Page>> {
        data.search_index.get(&page_id).cloned()
    }

    fn get_page_ptr_write(
        &self,
        data: &RwLockWriteGuard<CustomCacheEngineInner>,
        page_id: PageId,
    ) -> Option<Box<Page>> {
        data.search_index.get(&page_id).cloned()
    }

    fn get_next_free_page(
        &self,
        lock: &mut RwLockWriteGuard<CustomCacheEngineInner>,
        owner_id: String,
    ) -> Result<(PageId, Option<Box<Page>>)> {
        // Check if this owner has space left in their pages
        if let Some(free_pages) = lock.owner_free_pages_mapping.get_mut(&owner_id) {
            if let Some(&free_page) = free_pages.last() {
                free_pages.pop();
                let page = self.get_page_ptr_write(&lock, free_page);
                return Ok((free_page, page));
            }
        }

        // Otherwise, get an empty page
        if let Some(&last_index) = lock.free_pages.last() {
            lock.free_pages.pop();
            let page = self.get_page_ptr_write(&lock, last_index);
            return Ok((last_index, page));
        }

        // No empty pages, then
        if self.config.apply_lru_eviction {
            let replace_place_id = match lock.lru_main_vector.back() {
                Some(r) => *r,
                None => return Ok((-1, None)),
            };

            let mut page_to_reset = match self.get_page_ptr_write(&lock, replace_place_id) {
                Some(p) => p,
                None => return Ok((-1, None)),
            };
            let old_owner = page_to_reset.get_page_owner();
            let blocks = page_to_reset
                .allocated_block_ids
                .get_block_readable_offsets();

            for block_id in blocks.keys() {
                lock.owner_ordered_pages_mapping
                    .get_mut(&old_owner)
                    .and_then(|pages| pages.remove(&block_id));
            }
            lock.owner_free_pages_mapping
                .get_mut(&old_owner)
                .map(|pages| pages.remove(replace_place_id as usize));

            if page_to_reset.is_page_dirty() {
                page_to_reset.sync_data()?;
            }
            page_to_reset.reset();

            return Ok((replace_place_id, Some(page_to_reset)));
        }
        Ok((-1, None))
    }

    fn apply_lru_after_page_visitation_on_write(
        &self,
        lock: &mut RwLockWriteGuard<CustomCacheEngineInner>,
        visited_page_id: PageId,
    ) -> Result<()> {
        if let Some(&position) = lock.page_order_mapping.get(&visited_page_id) {
            lock.lru_main_vector.remove(position as usize);
        }

        lock.lru_main_vector.push_front(visited_page_id);
        let front_position = *lock.lru_main_vector.front().unwrap();
        lock.page_order_mapping
            .insert(visited_page_id, front_position);

        // If the LRU list is larger than the cache size, remove the least recently used page
        if lock.page_order_mapping.len() > self.config.cache_nr_pages as usize {
            if let Some(&back_page_id) = lock.lru_main_vector.back() {
                lock.page_order_mapping.remove(&back_page_id);
                lock.lru_main_vector.pop_back();
            }
        }
        Ok(())
    }

    fn apply_lru_after_page_visitation_on_read(
        &self,
        lock: &mut RwLockWriteGuard<CustomCacheEngineInner>,
        visited_page_id: PageId,
    ) {
        if let Some(&position) = lock.page_order_mapping.get(&visited_page_id) {
            lock.lru_main_vector.remove(position as usize);
        }

        // Add the visited page to the front of the LRU list
        lock.lru_main_vector.push_front(visited_page_id);
        let new_position = *lock.lru_main_vector.front().unwrap();
        lock.page_order_mapping
            .insert(visited_page_id, new_position);
    }

    fn update_owner_pages(
        &self,
        lock: &mut RwLockWriteGuard<CustomCacheEngineInner>,
        new_owner: String,
        page_id: PageId,
        block_id: BlockId,
        block_offsets_inside_page: Offsets,
    ) -> Result<()> {
        let mut page = match self.get_page_ptr_write(&lock, page_id) {
            Some(p) => p,
            None => return Ok(()),
        };

        let real_owner = page.get_page_owner();
        if real_owner == "none" || real_owner != new_owner {
            page.change_owner(new_owner.clone());

            // Erase old owner page mapping
            if lock.owner_pages_mapping.contains_key(&real_owner) {
                lock.owner_pages_mapping
                    .get_mut(&real_owner)
                    .unwrap()
                    .remove(&page_id);

                if lock.owner_ordered_pages_mapping.contains_key(&real_owner) {
                    lock.owner_ordered_pages_mapping
                        .get_mut(&real_owner)
                        .unwrap()
                        .remove(&block_id);
                }

                // Check if the owner's pages are now empty and remove the owner if so
                if lock
                    .owner_pages_mapping
                    .get(&real_owner)
                    .unwrap()
                    .is_empty()
                {
                    lock.owner_pages_mapping.remove(&real_owner);
                    lock.owner_free_pages_mapping.remove(&real_owner);
                    lock.owner_ordered_pages_mapping.remove(&real_owner);
                }
            }
        }

        lock.owner_pages_mapping
            .entry(new_owner.clone())
            .or_insert_with(HashSet::new)
            .insert(page_id);

        lock.owner_ordered_pages_mapping
            .entry(new_owner.clone())
            .or_insert_with(HashMap::new)
            .insert(
                block_id,
                (
                    page_id,
                    Box::new(*page.clone()),
                    block_offsets_inside_page,
                    false,
                ),
            );

        if page.has_free_space() {
            lock.owner_free_pages_mapping
                .entry(new_owner)
                .or_insert_with(Vec::new)
                .push(page_id);
        }

        Ok(())
    }
}

impl PageCacheEngine for CustomCacheEngine {
    fn allocate_blocks(
        &self,
        content_owner_id: String,
        block_data_mapping: HashMap<BlockId, (PageId, &Vec<u8>, i32)>,
        operation_type: AllocateOperationType,
    ) -> Result<HashMap<BlockId, PageId>> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        let mut res_block_allocated_pages = HashMap::new();

        for (&block_id, &(page_id, ref blk_data, offset_start)) in &block_data_mapping {
            if page_id >= 0 {
                if let Some(mut page) = self.get_page_ptr_write(&lock, page_id) {
                    if page.is_page_owner(&content_owner_id.clone()) && page.contains_block(block_id)
                    {
                        page.update_block_data(block_id, blk_data, offset_start as usize)?;
                        res_block_allocated_pages.insert(block_id, page_id);

                        self.update_owner_pages(
                            &mut lock,
                            content_owner_id.clone(),
                            page_id,
                            block_id,
                            (0, 0),
                        )?;

                        continue;
                    }
                }
            }

            let (free_page_id, free_page_ptr) =
                self.get_next_free_page(&mut lock, content_owner_id.clone())?;
            if free_page_id >= 0 {
                if let Some(mut page) = free_page_ptr {
                    let offs = page.get_allocate_free_offset(block_id)?;
                    page.update_block_data(block_id, blk_data, offset_start as usize)?;

                    if operation_type == AllocateOperationType::OpWrite {
                        page.set_page_as_dirty(true);
                    }

                    res_block_allocated_pages.insert(block_id, free_page_id);
                    self.apply_lru_after_page_visitation_on_write(&mut lock, free_page_id)?;

                    self.update_owner_pages(
                        &mut lock,
                        content_owner_id.clone(),
                        free_page_id,
                        block_id,
                        offs,
                    )?;
                } else {
                    res_block_allocated_pages.insert(block_id, -1);
                }
            } else {
                res_block_allocated_pages.insert(block_id, -1);
            }
        }

        Ok(res_block_allocated_pages)
    }

    fn get_blocks(
        &self,
        content_owner_id: String,
        block_pages: HashMap<BlockId, (PageId, Vec<u8>, i32)>,
    ) -> Result<HashMap<BlockId, bool>> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        let mut res_block_data = HashMap::new();

        for (block_id, (page_id, ref mut data, read_to_max_index)) in block_pages {
            if let Some(page) = self.get_page_ptr_write(&lock, page_id) {
                if page.is_page_owner(&content_owner_id) && page.contains_block(block_id) {
                    page.get_block_data(block_id, data.as_mut_slice(), read_to_max_index as usize)?;
                    res_block_data.insert(block_id, true);

                    if self.config.apply_lru_eviction {
                        self.apply_lru_after_page_visitation_on_read(&mut lock, page_id);
                    }
                } else {
                    res_block_data.insert(block_id, false);
                }
            } else {
                res_block_data.insert(block_id, false);
            }
        }

        Ok(res_block_data)
    }

    fn is_block_cached(
        &self,
        content_owner_id: String,
        page_id: PageId,
        block_id: BlockId,
    ) -> Result<bool> {
        let lock = self
            .data
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on data: {:?}", e))?;
        if let Some(page) = self.get_page_ptr_read(&lock, page_id) {
            return Ok(page.is_page_owner(&content_owner_id) && page.contains_block(block_id));
        }
        Ok(false)
    }

    fn make_block_readable_to_offset(
        &self,
        cid: String,
        page_id: PageId,
        block_id: BlockId,
        offset: i32,
    ) -> Result<()> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;
        let mut page = match self.get_page_ptr_write(&mut lock, page_id) {
            Some(p) => p,
            None => return Ok(()),
        };
        if page.is_page_owner(&cid) {
            page.make_block_readable_to(block_id, offset);
        }

        Ok(())
    }

    fn get_engine_usage(&self) -> Result<f64> {
        let lock = self
            .data
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on data: {:?}", e))?;
        let used_pages = self.config.cache_nr_pages - lock.free_pages.len();
        Ok((used_pages as f64 / self.config.cache_nr_pages as f64) * 100.0)
    }

    fn remove_cached_blocks(&self, owner: String) -> Result<bool> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        lock.owner_free_pages_mapping.remove(&owner);

        // Process each page owned by the owner
        if let Some(owner_pgs) = lock.owner_pages_mapping.remove(&owner) {
            for page_id in owner_pgs {
                // Add the page back to the list of free pages
                lock.free_pages.push(page_id);

                // Apply LRU eviction logic if enabled
                if self.config.apply_lru_eviction {
                    if let Some(position) = lock.page_order_mapping.remove(&page_id) {
                        lock.lru_main_vector.remove(position as usize);
                    }
                }

                // Reset the page and change its owner to "none"
                if let Some(mut page_ptr) = self.get_page_ptr_write(&mut lock, page_id) {
                    page_ptr.reset();
                    page_ptr.change_owner("none".to_string());
                }
            }
            lock.owner_ordered_pages_mapping.remove(&owner);
        }

        Ok(true)
    }

    fn sync_pages(&self, owner: String, size: u32, orig_path: String) -> Result<()> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        let mut fd = OpenOptions::new().write(true).open(orig_path)?;

        if let Some(iterate_blocks) = lock.owner_ordered_pages_mapping.get_mut(&owner) {
            // let mut wrote_bytes = 0;
            let mut page_streak = 0;

            let mut new_iterate_blocks: HashMap<i32, (i32, Page, Offsets, bool)> = HashMap::new();

            for (index, (_, (page_id, page, offsets, flag))) in
                iterate_blocks.iter_mut().enumerate()
            {
                if page.is_page_dirty() {
                    new_iterate_blocks
                        .insert(index as i32, (*page_id, *page.clone(), *offsets, *flag));
                    page.set_page_as_dirty(false);
                }
            }

            let mut page_streak_last_offset =
                new_iterate_blocks.keys().next().unwrap() * (self.config.io_block_size as i32);

            let mut page_chunk: Vec<(i32, Page, Offsets, bool)> =
                Vec::with_capacity(new_iterate_blocks.len());
            page_chunk.extend(new_iterate_blocks.values().cloned());

            for (current_block_id, page_data) in new_iterate_blocks.iter() {
                let next_block_id = match new_iterate_blocks.get(&(current_block_id + 1)) {
                    Some(data) => data.0,
                    None => continue,
                };

                if (*current_block_id as usize) != (new_iterate_blocks.len() - 1)
                    && *current_block_id == (next_block_id - 1)
                {
                    page_streak += 1;
                    page_chunk.push((page_data.0, page_data.1.clone(), page_data.2, page_data.3));
                } else {
                    page_streak += 1;
                    page_chunk.push((page_data.0, page_data.1.clone(), page_data.2, page_data.3));

                    let mut buffer = Vec::new();
                    let mut cursor = Cursor::new(&mut buffer);

                    page_streak_last_offset =
                        (current_block_id - page_streak + 1) * self.config.io_block_size as i32;

                    for p_id in 0..page_streak {
                        let streak_block = current_block_id - page_streak + p_id + 1;

                        let streak_pair = &mut page_chunk[p_id as usize];
                        let page_ptr = &streak_pair.1;
                        let block_data_offs = &streak_pair.2;

                        let data = &page_ptr.data[block_data_offs.0 as usize..];

                        if p_id == page_streak - 1 {
                            let readable_to =
                                page_ptr.allocated_block_ids.get_readable_to(streak_block) + 1;
                            cursor.write_all(&data[..readable_to as usize])?;
                        } else {
                            cursor.write_all(&data[..self.config.io_block_size])?;
                        }

                        streak_pair.3 = true;
                    }

                    fd.seek(SeekFrom::Start(page_streak_last_offset as u64))?;
                    fd.write(&buffer)?;
                    // wrote_bytes += fd.write(&buffer)?;

                    page_streak = 0;
                    page_chunk.clear();
                    page_streak_last_offset =
                        (current_block_id + 1) * self.config.io_block_size as i32;
                }
            }
        }

        // Truncate the file to the specified size
        fd.set_len(size as u64)?;

        Ok(())
    }

    fn rename_owner_pages(&self, old_owner: String, new_owner: String) -> Result<bool> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        // Check if the old owner exists in the mapping
        if !lock.owner_pages_mapping.contains_key(&old_owner) {
            return Ok(false);
        }

        // Retrieve the old owner's data
        let old_page_mapping = lock.owner_pages_mapping.remove(&old_owner).unwrap();
        let old_free_mapping = lock.owner_free_pages_mapping.remove(&old_owner).unwrap();
        let old_ordered_pages = lock.owner_ordered_pages_mapping.remove(&old_owner).unwrap();

        // Change the owner of each page
        for &page_id in &old_page_mapping {
            if let Some(mut page) = self.get_page_ptr_write(&mut lock, page_id) {
                page.change_owner(new_owner.clone());
            }
        }

        // Update the mappings for the new owner
        lock.owner_pages_mapping
            .insert(new_owner.clone(), old_page_mapping);
        lock.owner_free_pages_mapping
            .insert(new_owner.clone(), old_free_mapping);
        lock.owner_ordered_pages_mapping
            .insert(new_owner, old_ordered_pages);

        Ok(true)
    }

    fn truncate_cached_blocks(
        &self,
        content_owner_id: String,
        blocks_to_remove: HashMap<BlockId, PageId>,
        from_block_id: BlockId,
        index_inside_block: i32,
    ) -> Result<bool> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| anyhow!("Failed to acquire write lock on data: {:?}", e))?;

        for (&block_id, &page_id) in &blocks_to_remove {
            if let Some(mut page) = self.get_page_ptr_write(&mut lock, page_id) {
                if page.is_page_owner(&content_owner_id) {
                    if block_id == from_block_id && index_inside_block > 0 {
                        if page.contains_block(from_block_id) {
                            page.make_block_readable_to(from_block_id, index_inside_block - 1);
                            page.write_null_from(from_block_id, index_inside_block);
                        }
                    } else {
                        page.remove_block(block_id);

                        if let Some(owner_pages) =
                            lock.owner_pages_mapping.get_mut(&content_owner_id)
                        {
                            owner_pages.remove(&page_id);
                        }
                        if let Some(ordered_pages) =
                            lock.owner_ordered_pages_mapping.get_mut(&content_owner_id)
                        {
                            ordered_pages.remove(&block_id);
                        }

                        if !page.is_page_dirty() {
                            lock.free_pages.push(page_id);
                            if self.config.apply_lru_eviction {
                                if let Some(position) = lock.page_order_mapping.remove(&page_id) {
                                    lock.lru_main_vector.remove(position as usize);
                                }
                            }
                            page.reset();
                            page.change_owner("none".to_string());
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    fn get_dirty_blocks_info(&self, owner: String) -> Result<Vec<(BlockId, Offsets, PageId)>> {
        let lock = self
            .data
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock on data: {:?}", e))?;
        let mut res = Vec::new();
        if let Some(ordered_pages) = lock.owner_ordered_pages_mapping.get(&owner) {
            for (&block_id, &(page_id, ref page, _, is_synced)) in ordered_pages {
                if !is_synced {
                    let offs = (0, page.allocated_block_ids.get_readable_to(block_id));
                    res.push((block_id, offs, page_id));
                }
            }
        }
        Ok(res)
    }
}
