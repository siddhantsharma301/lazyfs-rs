use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::pagecache::{cache, config};

pub struct LazyFS {
    cache: cache::Cache,
    config: config::Config,
    faults: HashMap<String, Vec<Arc<dyn config::Fault>>>,
    crash_faults: HashMap<String, HashSet<String>>,
    pending_write: Mutex<Write>,
    path_injecting_fault: Mutex<PathBuf>,
}

impl LazyFS {
    //     pub fn new(cache: cache::Cache, config: config::Config)
    pub fn get_path_injecting_fault(&self) -> PathBuf {
        let lock = self.path_injecting_fault.lock().unwrap();
        lock.clone()
    }

    
}

struct Write {
    path: PathBuf,
    buf: Vec<u8>,
    offset: u32,
}

impl Write {
    pub fn new(path: PathBuf, buf: Vec<u8>, offset: u32) -> Write {
        Write { path, buf, offset }
    }
}
