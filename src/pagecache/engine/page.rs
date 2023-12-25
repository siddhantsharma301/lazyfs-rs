use crate::pagecache::config::Config;
use crate::pagecache::engine::block_offsets::BlockOffsets;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::vec::Vec;

#[derive(Debug)]
pub struct Page {
    is_dirty: bool,
    page_owner_id: String,
    free_block_indexes: Vec<i32>,
    config: Box<Config>,               // Assuming Config is defined elsewhere
    data: Vec<u8>,                     // Rust's safe alternative to raw char* pointers
    allocated_block_ids: BlockOffsets, // Assuming BlockOffsets is a struct defined elsewhere
}

impl Page {
    fn new(config: Box<Config>) -> Result<Self> {
        assert!(config.cache_page_size % config.io_block_size == 0);

        let mut page = Page {
            is_dirty: false,
            page_owner_id: "none".to_string(),
            free_block_indexes: Vec::with_capacity(config.cache_page_size / config.io_block_size),
            config,
            data: vec![0; config.cache_page_size],
            allocated_block_ids: BlockOffsets::default(),
        };

        for i in (0..config.cache_page_size).step_by(config.io_block_size) {
            page.free_block_indexes.push(i as i32);
        }

        page.allocated_block_ids.with_capacity(10);
        Ok(page)
    }

    fn is_page_owner(&self, query: &str) -> bool {
        self.page_owner_id == query
    }

    fn change_owner(&mut self, new_owner: String) {
        self.page_owner_id = new_owner;
    }

    fn get_page_owner(&self) -> &str {
        &self.page_owner_id
    }

    fn has_free_space(&self) -> bool {
        !self.free_block_indexes.is_empty()
    }

    fn reset(&mut self) {
        self.free_block_indexes.clear();
        self.allocated_block_ids.reset();
        self.is_dirty = false;
        self.data.fill(0);
        for i in (0..self.config.cache_page_size).step_by(self.config.io_block_size) {
            self.free_block_indexes.push(i as i32);
        }
    }

    fn update_block_data(
        &mut self,
        block_id: i32,
        new_data: &[u8],
        data_length: usize,
        off_start: usize,
    ) -> Result<bool> {
        todo!()
    }

    fn rewrite_offset_data(&mut self, new_data: &[u8], start: usize, end: usize) {
        self.set_page_as_dirty(true);
        for (i, &byte) in new_data.iter().enumerate() {
            self.data[start + i] = byte;
        }
    }

    fn get_allocate_free_offset(&mut self, block_id: i32) -> Result<(i32, i32)> {
        if let Some(&free_index) = self.free_block_indexes.last() {
            self.free_block_indexes.pop();

            let allocated_offset = (
                free_index,
                free_index + self.config.io_block_size as i32 - 1,
            );
            self.allocated_block_ids
                .insert_or_update_block_offsets(block_id, allocated_offset);

            Ok(allocated_offset)
        } else {
            Err(anyhow!("No free block indexes available"))
        }
    }

    fn get_block_data(
        &self,
        block_id: i32,
        buffer: &mut [u8],
        read_to_max_index: usize,
    ) -> Result<()> {
        let (off_min, _) = self.get_block_offsets(block_id);
        let off_max = read_to_max_index + 1;

        if off_max <= self.data.len()
            && off_min < off_max as i32
            && (off_max - off_min as usize) <= buffer.len()
        {
            buffer[..off_max - off_min as usize]
                .copy_from_slice(&self.data[off_min as usize..off_max]);
            Ok(())
        } else {
            Err(anyhow!("Invalid offset or buffer size"))
        }
    }

    // TODO: i dont know if this is correct, need to check if this is how i can use fuse
    fn sync_data(&self) -> Result<bool> {
        let path = &self.page_owner_id;
        let mut file = OpenOptions::new().write(true).open(path)?;

        let block_readable_offsets = self.allocated_block_ids.get_block_readable_offsets();

        let mut should_write = 0;
        let mut actually_wrote = 0;

        for (&block_id, &max_offset) in &block_readable_offsets {
            if self.contains_block(block_id) {
                if let (offset_start, _) = self.get_block_offsets(block_id) {
                    let offset = block_id as u64 * self.config.io_block_size as u64;
                    let total_bytes = self.config.io_block_size;
                    should_write += total_bytes;

                    file.seek(SeekFrom::Start(offset))?;
                    let bytes_to_write = &self.data
                        [offset_start as usize..(offset_start + total_bytes as i32) as usize];
                    actually_wrote += file.write(bytes_to_write)?;
                }
            }
        }

        let res = should_write == actually_wrote;
        if res {
            self.is_dirty = false;
        }

        Ok(res)
    }

    fn is_page_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_page_as_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }

    fn make_block_readable_to(&mut self, blk_id: i32, max_offset: i32) {
        self.allocated_block_ids
            .make_readable_to(blk_id, max_offset);
    }

    fn write_null_from(&mut self, block_id: i32, from_offset: usize) {
        let (off_first, _) = self.get_block_offsets(block_id);
        let range = off_first + (from_offset as i32)..(self.config.io_block_size as i32);
        for i in range {
            self.data[i as usize] = 0;
        }
    }

    fn remove_block(&mut self, block_id: i32) {
        if self.contains_block(block_id) {
            let (off_first, _) = self.get_block_offsets(block_id);
            self.free_block_indexes.push(off_first as i32);
            self.allocated_block_ids.remove_block(block_id);
            for i in off_first..off_first + (self.config.io_block_size as i32) {
                self.data[i as usize] = 0;
            }
        }

        if self.allocated_block_ids.empty() {
            self.is_dirty = false;
        }
    }

    fn get_block_offsets(&self, block_id: i32) -> (i32, i32) {
        self.allocated_block_ids.get_block_offsets(block_id)
    }

    fn contains_block(&self, block_id: i32) -> bool {
        self.allocated_block_ids.contains_block(block_id)
    }
}
