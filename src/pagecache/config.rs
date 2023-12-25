use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::AtomicI32;
use toml;

pub trait Fault {}

pub struct SplitWriteFault {
    occurence: i32,
    counter: AtomicI32,
    persist: Vec<i32>,
    parts: i32,
    parts_bytes: Vec<i32>,
}

impl SplitWriteFault {
    pub fn from_parts(occurence: i32, persist: Vec<i32>, parts: i32) -> Self {
        SplitWriteFault {
            occurence,
            counter: AtomicI32::new(0),
            persist,
            parts,
            parts_bytes: Vec::new(),
        }
    }

    pub fn from_parts_bytes(occurence: i32, persist: Vec<i32>, parts_bytes: Vec<i32>) -> Self {
        SplitWriteFault {
            occurence,
            counter: AtomicI32::new(0),
            persist,
            parts: 0,
            parts_bytes,
        }
    }
}

impl Fault for SplitWriteFault {}

impl Default for SplitWriteFault {
    fn default() -> Self {
        Self {
            occurence: 0,
            counter: AtomicI32::new(0),
            persist: Vec::new(),
            parts: 0,
            parts_bytes: Vec::new(),
        }
    }
}

pub struct ReorderFault {
    op: String,
    occurence: i32,
    counter: AtomicI32,
    persist: Vec<i32>,
    group_counter: AtomicI32,
}

impl ReorderFault {
    pub fn from_op(op: String, persist: Vec<i32>, occurence: i32) -> Self {
        ReorderFault {
            op,
            occurence,
            counter: AtomicI32::new(0),
            persist,
            group_counter: AtomicI32::new(0),
        }
    }
}

impl Fault for ReorderFault {}

impl Default for ReorderFault {
    fn default() -> Self {
        Self {
            op: "".to_string(),
            occurence: 0,
            counter: AtomicI32::new(0),
            persist: Vec::new(),
            group_counter: AtomicI32::new(0),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub log_all_operations: bool,
    pub is_default_config: bool,
    pub cache_nr_pages: usize,
    pub cache_page_size: usize,
    pub io_block_size: usize,
    pub disk_sector_size: usize,
    pub apply_lru_eviction: bool,
    pub fifo_path: PathBuf,
    pub fifo_path_completed: PathBuf,
    pub log_file: PathBuf,
}

impl Config {
    fn from_size(&mut self, prealloc_bytes: usize, nr_blocks_per_page: i32) -> Result<()> {
        self.is_default_config = false;

        if prealloc_bytes < self.io_block_size {
            return Err(anyhow!("error mcerror"));
        }

        let total_bytes = (prealloc_bytes as f64).ceil() as usize;
        self.cache_page_size = nr_blocks_per_page as usize * self.io_block_size;

        if total_bytes <= (nr_blocks_per_page as usize * self.io_block_size) {
            return Err(anyhow!("total bytes is more than what can be stored"));
        }

        self.cache_nr_pages = total_bytes / self.cache_page_size;

        Ok(())
    }

    fn setup_config_manually(
        &mut self,
        io_blk_sz: usize,
        pg_sz: usize,
        nr_pgs: usize,
    ) -> Result<()> {
        if nr_pgs == 0 {
            return Err(anyhow!("number of pages must be != 0"));
        }
        if pg_sz % io_blk_sz != 0 {
            return Err(anyhow!("page size must be multiple of IO block size"));
        }

        self.cache_nr_pages = nr_pgs;
        self.cache_page_size = pg_sz;
        self.io_block_size = io_blk_sz;

        Ok(())
    }

    pub fn new_with_manual_config(io_blk_sz: usize, pg_sz: usize, nr_pgs: usize) -> Result<Self> {
        let mut config = Self::default();
        config.setup_config_manually(io_blk_sz, pg_sz, nr_pgs)?;
        Ok(config)
    }

    pub fn new_with_size(prealloc_bytes: usize, nr_blocks_per_page: i32) -> Result<Self> {
        let mut config = Self::default();
        config.from_size(prealloc_bytes, nr_blocks_per_page)?;
        Ok(config)
    }

    pub fn set_eviction_flag(&mut self, flag: bool) {
        self.apply_lru_eviction = flag;
    }

    pub fn load_config(filename: &str) -> Result<Config> {
        let mut file = File::open(filename)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config: Config = toml::from_str(&contents)?;

        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_all_operations: false,
            is_default_config: true,
            cache_nr_pages: 5,
            cache_page_size: 4096,
            io_block_size: 4096,
            disk_sector_size: 512,
            apply_lru_eviction: false,
            fifo_path: "faults.fifo".to_string().into(),
            fifo_path_completed: "".to_string().into(),
            log_file: "".to_string().into(),
        }
    }
}
