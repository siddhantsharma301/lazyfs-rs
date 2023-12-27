use crate::pagecache::config::Config;
use crate::pagecache::engine::page::Page;
use crate::pagecache::engine::{AllocateOperationType, PageCacheEngine};
use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{Mutex, RwLock};

#[derive(Debug)]
pub struct CustomCacheEngine {
    config: Box<Config>,
    data: RwLock<CustomCacheEngineInner>,
    lru: Mutex<CustomCacheLRU>,
}

#[derive(Debug)]
pub(crate) struct CustomCacheEngineInner {
    search_index: HashMap<i32, Box<Page>>,
    free_pages: Vec<i32>,
    owner_pages_mapping: HashMap<String, HashSet<i32>>,
    owner_ordered_pages_mapping: HashMap<String, HashMap<i32, (i32, Box<Page>, (i32, i32), bool)>>,
    owner_free_pages_mapping: HashMap<String, Vec<i32>>,
}

impl CustomCacheEngineInner {
    pub fn new() -> Self {
        CustomCacheEngineInner {
            search_index: HashMap::new(),
            free_pages: Vec::new(),
            owner_pages_mapping: HashMap::new(),
            owner_ordered_pages_mapping: HashMap::new(),
            owner_free_pages_mapping: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct CustomCacheLRU {
    lru_main_vector: VecDeque<i32>,
    page_order_mapping: HashMap<i32, i32>,
}

impl CustomCacheLRU {
    pub fn new() -> Self {
        CustomCacheLRU {
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
            lru: Mutex::new(CustomCacheLRU::new()),
        }
    }

    fn get_page_ptr(&self, page_id: i32) -> Option<Box<Page>> {
        self.data
            .read()
            .unwrap()
            .search_index
            .get(&page_id)
            .cloned()
    }

    fn has_empty_pages(&self) -> bool {
        self.data.read().unwrap().free_pages.len() > 0
    }

    fn get_next_free_page(&mut self, owner_id: String) -> Result<(i32, Option<Box<Page>>)> {
        // Check if this owner has space left in their pages
        let mut lock = self.data.write().unwrap();
        if let Some(free_pages) = lock.owner_free_pages_mapping.get_mut(&owner_id) {
            if let Some(&free_page) = free_pages.last() {
                free_pages.pop();
                let page = self.get_page_ptr(free_page);
                return Ok((free_page, page));
            }
        }

        // Otherwise, get an empty page
        if let Some(&last_index) = lock.free_pages.last() {
            lock.free_pages.pop();
            let page = self.get_page_ptr(last_index);
            return Ok((last_index, page));
        }

        // No empty pages, then
        if self.config.apply_lru_eviction {
            let lru_lock = self.lru.lock().unwrap();
            let replace_place_id = match lru_lock.lru_main_vector.back() {
                Some(r) => *r,
                None => return Ok((-1, None)),
            };
            drop(lru_lock);

            let mut page_to_reset = match self.get_page_ptr(replace_place_id) {
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

    fn apply_lru_after_page_visitation_on_write(&mut self, visited_page_id: i32) {
        let mut lru_lock = self.lru.lock().unwrap();
        if let Some(&position) = lru_lock.page_order_mapping.get(&visited_page_id) {
            lru_lock.lru_main_vector.remove(position as usize);
        }

        lru_lock.lru_main_vector.push_front(visited_page_id);
        let front_position = *lru_lock.lru_main_vector.front().unwrap();
        lru_lock
            .page_order_mapping
            .insert(visited_page_id, front_position);

        // If the LRU list is larger than the cache size, remove the least recently used page
        if lru_lock.page_order_mapping.len() > self.config.cache_nr_pages as usize {
            if let Some(&back_page_id) = lru_lock.lru_main_vector.back() {
                lru_lock.page_order_mapping.remove(&back_page_id);
                lru_lock.lru_main_vector.pop_back();
            }
        }
    }

    fn apply_lru_after_page_visitation_on_read(&mut self, visited_page_id: i32) {
        let mut lru_lock = self.lru.lock().unwrap();
        if let Some(&position) = lru_lock.page_order_mapping.get(&visited_page_id) {
            lru_lock.lru_main_vector.remove(position as usize);
        }

        // Add the visited page to the front of the LRU list
        lru_lock.lru_main_vector.push_front(visited_page_id);
        let new_position = *lru_lock.lru_main_vector.front().unwrap();
        lru_lock
            .page_order_mapping
            .insert(visited_page_id, new_position);
    }

    fn update_owner_pages(
        &mut self,
        new_owner: String,
        page_id: i32,
        block_id: i32,
        block_offsets_inside_page: (i32, i32),
    ) {
        let mut page = match self.get_page_ptr(page_id) {
            Some(p) => p,
            None => return,
        };

        let mut lock = self.data.write().unwrap();

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
    }
}

impl PageCacheEngine for CustomCacheEngine {
    fn allocate_blocks(
        &mut self,
        content_owner_id: String,
        block_data_mapping: HashMap<i32, (i32, &[u8], usize, i32)>,
        operation_type: AllocateOperationType,
    ) -> Result<HashMap<i32, i32>> {
        let mut res_block_allocated_pages = HashMap::new();

        for (&blk_id, &(page_id, ref blk_data, blk_data_len, offset_start)) in &block_data_mapping {
            if page_id >= 0 {
                if let Some(mut page) = self.get_page_ptr(page_id) {
                    if page.is_page_owner(&content_owner_id.clone()) && page.contains_block(blk_id)
                    {
                        page.update_block_data(
                            blk_id,
                            blk_data,
                            blk_data_len,
                            offset_start as usize,
                        )?;
                        res_block_allocated_pages.insert(blk_id, page_id);

                        self.update_owner_pages(content_owner_id.clone(), page_id, blk_id, (0, 0));

                        continue;
                    }
                }
            }

            let (free_page_id, free_page_ptr) =
                self.get_next_free_page(content_owner_id.clone())?;
            if free_page_id >= 0 {
                if let Some(mut page) = free_page_ptr {
                    let offs = page.get_allocate_free_offset(blk_id)?;
                    page.update_block_data(blk_id, blk_data, blk_data_len, offset_start as usize)?;

                    if operation_type == AllocateOperationType::OpWrite {
                        page.set_page_as_dirty(true);
                    }

                    res_block_allocated_pages.insert(blk_id, free_page_id);
                    self.apply_lru_after_page_visitation_on_write(free_page_id);

                    self.update_owner_pages(content_owner_id.clone(), free_page_id, blk_id, offs);
                } else {
                    res_block_allocated_pages.insert(blk_id, -1);
                }
            } else {
                res_block_allocated_pages.insert(blk_id, -1);
            }
        }

        Ok(res_block_allocated_pages)
    }

    fn get_blocks(
        &mut self,
        content_owner_id: String,
        block_pages: HashMap<i32, (i32, Vec<u8>, i32)>,
    ) -> Result<HashMap<i32, bool>> {
        let mut res_block_data = HashMap::new();

        for (blk_id, (page_id, ref mut data, read_to_max_index)) in block_pages {
            if let Some(page) = self.get_page_ptr(page_id) {
                if page.is_page_owner(&content_owner_id) && page.contains_block(blk_id) {
                    page.get_block_data(blk_id, data.as_mut_slice(), read_to_max_index as usize)?;
                    res_block_data.insert(blk_id, true);

                    if self.config.apply_lru_eviction {
                        self.apply_lru_after_page_visitation_on_read(page_id);
                    }
                } else {
                    res_block_data.insert(blk_id, false);
                }
            } else {
                res_block_data.insert(blk_id, false);
            }
        }

        Ok(res_block_data)
    }

    fn is_block_cached(&self, content_owner_id: String, page_id: i32, block_id: i32) -> bool {
        if let Some(page) = self.get_page_ptr(page_id) {
            return page.is_page_owner(&content_owner_id) && page.contains_block(block_id);
        }
        false
    }

    fn make_block_readable_to_offset(
        &mut self,
        cid: String,
        page_id: i32,
        block_id: i32,
        offset: i32,
    ) {
        let _lock = self.data.write().unwrap();
        let mut page = match self.get_page_ptr(page_id) {
            Some(p) => p,
            None => return,
        };
        if page.is_page_owner(&cid) {
            page.make_block_readable_to(block_id, offset);
        }
    }

    fn get_engine_usage(&self) -> f64 {
        let lock = self.data.read().unwrap();
        let used_pages = self.config.cache_nr_pages - lock.free_pages.len();
        (used_pages as f64 / self.config.cache_nr_pages as f64) * 100.0
    }

    fn remove_cached_blocks(&mut self, owner: String) -> bool {
        let mut lock = self.data.write().unwrap();

        lock.owner_free_pages_mapping.remove(&owner);

        // Process each page owned by the owner
        if let Some(owner_pgs) = lock.owner_pages_mapping.remove(&owner) {
            for page_id in owner_pgs {
                // Add the page back to the list of free pages
                lock.free_pages.push(page_id);

                // Apply LRU eviction logic if enabled
                if self.config.apply_lru_eviction {
                    let mut lru_lock = self.lru.lock().unwrap();
                    if let Some(position) = lru_lock.page_order_mapping.remove(&page_id) {
                        lru_lock.lru_main_vector.remove(position as usize);
                    }
                }

                // Reset the page and change its owner to "none"
                if let Some(mut page_ptr) = self.get_page_ptr(page_id) {
                    page_ptr.reset();
                    page_ptr.change_owner("none".to_string());
                }
            }
            lock.owner_ordered_pages_mapping.remove(&owner);
        }

        true
    }

    fn sync_pages(&mut self, owner: String, size: u64, orig_path: String) -> Result<()> {
        let mut lock = self.data.write().unwrap();

        let mut fd = OpenOptions::new().write(true).open(orig_path)?;

        if let Some(iterate_blocks) = lock.owner_ordered_pages_mapping.get_mut(&owner) {
            // let mut wrote_bytes = 0;
            let mut page_streak = 0;

            let mut new_iterate_blocks: HashMap<i32, (i32, Page, (i32, i32), bool)> =
                HashMap::new();

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

            let mut page_chunk: Vec<(i32, Page, (i32, i32), bool)> =
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
        fd.set_len(size)?;

        Ok(())
    }

    fn rename_owner_pages(&mut self, old_owner: String, new_owner: String) -> bool {
        let mut lock = self.data.write().unwrap();

        // Check if the old owner exists in the mapping
        if !lock.owner_pages_mapping.contains_key(&old_owner) {
            return false;
        }

        // Retrieve the old owner's data
        let old_page_mapping = lock.owner_pages_mapping.remove(&old_owner).unwrap();
        let old_free_mapping = lock.owner_free_pages_mapping.remove(&old_owner).unwrap();
        let old_ordered_pages = lock.owner_ordered_pages_mapping.remove(&old_owner).unwrap();

        // Change the owner of each page
        for &page_id in &old_page_mapping {
            if let Some(mut page) = self.get_page_ptr(page_id) {
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

        true
    }

    fn truncate_cached_blocks(
        &mut self,
        content_owner_id: String,
        blocks_to_remove: HashMap<i32, i32>,
        from_block_id: i32,
        index_inside_block: i32,
    ) -> bool {
        let mut lock = self.data.write().unwrap();

        for (&blk_id, &page_id) in &blocks_to_remove {
            if let Some(mut page) = self.get_page_ptr(page_id) {
                if page.is_page_owner(&content_owner_id) {
                    if blk_id == from_block_id && index_inside_block > 0 {
                        if page.contains_block(from_block_id) {
                            page.make_block_readable_to(from_block_id, index_inside_block - 1);
                            page.write_null_from(from_block_id, index_inside_block);
                        }
                    } else {
                        page.remove_block(blk_id);

                        if let Some(owner_pages) =
                            lock.owner_pages_mapping.get_mut(&content_owner_id)
                        {
                            owner_pages.remove(&page_id);
                        }
                        if let Some(ordered_pages) =
                            lock.owner_ordered_pages_mapping.get_mut(&content_owner_id)
                        {
                            ordered_pages.remove(&blk_id);
                        }

                        if !page.is_page_dirty() {
                            lock.free_pages.push(page_id);
                            if self.config.apply_lru_eviction {
                                let mut lru_lock = self.lru.lock().unwrap();
                                if let Some(position) = lru_lock.page_order_mapping.remove(&page_id)
                                {
                                    lru_lock.lru_main_vector.remove(position as usize);
                                }
                            }
                            page.reset();
                            page.change_owner("none".to_string());
                        }
                    }
                }
            }
        }

        true
    }

    fn get_dirty_blocks_info(&self, owner: String) -> Vec<(i32, (i32, i32), i32)> {
        let lock = self.data.read().unwrap();
        let mut res = Vec::new();
        if let Some(ordered_pages) = lock.owner_ordered_pages_mapping.get(&owner) {
            for (&block_id, &(page_id, ref page, _, is_synced)) in ordered_pages {
                if !is_synced {
                    let offs = (0, page.allocated_block_ids.get_readable_to(block_id));
                    res.push((block_id, offs, page_id));
                }
            }
        }
        res
    }
}
