use crate::pagecache::config::Config;
use crate::pagecache::engine::block_offsets::BlockOffsets;
use crate::pagecache::{BlockId, Offsets};
use anyhow::{anyhow, Result};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Page {
    is_dirty: bool,
    page_owner_id: String,
    free_block_indexes: Vec<i32>,
    config: Box<Config>,
    pub data: Vec<u8>,
    pub allocated_block_ids: BlockOffsets,
}

impl Page {
    fn new(config: Box<Config>) -> Result<Self> {
        if config.cache_page_size % config.io_block_size != 0 {
            return Err(anyhow!(
                "Cache page size must be divisible by IO block size"
            ));
        }

        let cache_page_size = config.cache_page_size;
        let io_block_size = config.io_block_size;

        let mut page = Page {
            is_dirty: false,
            page_owner_id: "none".to_string(),
            free_block_indexes: Vec::with_capacity(config.cache_page_size / config.io_block_size),
            config,
            data: vec![0; cache_page_size],
            allocated_block_ids: BlockOffsets::default(),
        };

        for i in (0..cache_page_size).step_by(io_block_size) {
            page.free_block_indexes.push(i as i32);
        }

        page.allocated_block_ids.with_capacity(10);
        Ok(page)
    }

    pub fn is_page_owner(&self, query: &str) -> bool {
        self.page_owner_id == query
    }

    pub fn change_owner(&mut self, new_owner: String) {
        self.page_owner_id = new_owner;
    }

    pub fn get_page_owner(&self) -> String {
        self.page_owner_id.clone()
    }

    pub fn has_free_space(&self) -> bool {
        !self.free_block_indexes.is_empty()
    }

    pub fn reset(&mut self) {
        self.free_block_indexes.clear();
        self.allocated_block_ids.reset();
        self.is_dirty = false;
        self.data.fill(0);
        for i in (0..self.config.cache_page_size).step_by(self.config.io_block_size) {
            self.free_block_indexes.push(i as i32);
        }
    }

    pub fn update_block_data(
        &mut self,
        block_id: BlockId,
        new_data: &Vec<u8>,
        off_start: usize,
    ) -> Result<bool> {
        let block_offsets = self.get_block_offsets(block_id);
        let off_min = block_offsets.0;
        if block_offsets.0 >= 0 && block_offsets.1 > 0 {
            if new_data.len() > self.config.io_block_size {
                return Err(anyhow!("Data length must be less than IO block size"));
            }
            self.rewrite_offset_data(
                new_data,
                off_min as usize + off_start,
                off_min as usize + off_start + new_data.len() - 1,
            );
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn rewrite_offset_data(&mut self, new_data: &[u8], start: usize, end: usize) {
        self.set_page_as_dirty(true);
        for (i, &byte) in new_data.iter().enumerate() {
            self.data[start + i] = byte;
        }
    }

    pub fn get_allocate_free_offset(&mut self, block_id: BlockId) -> Result<Offsets> {
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

    pub fn get_block_data(
        &self,
        block_id: BlockId,
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
    pub fn sync_data(&mut self) -> Result<bool> {
        let path = &self.page_owner_id;
        let mut file = OpenOptions::new().write(true).open(path)?;

        let block_readable_offsets = self.allocated_block_ids.get_block_readable_offsets();

        let mut should_write = 0;
        let mut actually_wrote = 0;

        for &block_id in block_readable_offsets.keys() {
            if self.contains_block(block_id) {
                let (offset_start, _) = self.get_block_offsets(block_id);
                let offset = block_id as u64 * self.config.io_block_size as u64;
                let total_bytes = self.config.io_block_size;
                should_write += total_bytes;

                file.seek(SeekFrom::Start(offset))?;
                let bytes_to_write =
                    &self.data[offset_start as usize..(offset_start + total_bytes as i32) as usize];
                actually_wrote += file.write(bytes_to_write)?;
            }
        }

        let res = should_write == actually_wrote;
        if res {
            self.is_dirty = false;
        }

        Ok(res)
    }

    pub fn is_page_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_page_as_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }

    pub fn make_block_readable_to(&mut self, block_id: BlockId, max_offset: i32) {
        self.allocated_block_ids
            .make_readable_to(block_id, max_offset);
    }

    pub fn write_null_from(&mut self, block_id: BlockId, from_offset: i32) {
        let (off_first, _) = self.get_block_offsets(block_id);
        let range = off_first + (from_offset as i32)..(self.config.io_block_size as i32);
        for i in range {
            self.data[i as usize] = 0;
        }
    }

    pub fn remove_block(&mut self, block_id: BlockId) {
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

    fn get_block_offsets(&self, block_id: BlockId) -> Offsets {
        self.allocated_block_ids.get_block_offsets(block_id)
    }

    pub fn contains_block(&self, block_id: BlockId) -> bool {
        self.allocated_block_ids.contains_block(block_id)
    }
}
