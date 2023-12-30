use anyhow::{anyhow, Result};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::pagecache::{cache, config};

const ALLOW_CRASH_FS_OPERATIONS: [&str; 11] = [
    "unlink", "truncate", "fsync", "write", "create", "access", "open", "read", "rename", "link",
    "symlink",
];

pub struct LazyFS {
    cache: cache::Cache,
    config: config::Config,
    faults: HashMap<String, Vec<Arc<dyn config::Fault>>>,
    crash_faults: HashMap<String, HashSet<String>>,
    pending_write: Mutex<Write>,
    path_injecting_fault: Mutex<PathBuf>,

    pub crash_faults_before: HashMap<String, Vec<(Regex, String)>>,
    pub crash_faults_after: HashMap<String, Vec<(Regex, String)>>,

    allow_crash_fs_ops: HashSet<String>,
    fs_op_mult_path: HashSet<String>,
}

impl LazyFS {
    pub fn new(
        cache: cache::Cache,
        config: config::Config,
        _faults_handler_thread: std::thread::Thread,
        _fht_worker: fn(&LazyFS),
        faults: HashMap<String, Vec<Arc<dyn config::Fault>>>,
    ) -> LazyFS {
        let mut crash_faults_before = HashMap::new();
        let mut crash_faults_after = HashMap::new();

        for op in ALLOW_CRASH_FS_OPERATIONS.iter() {
            crash_faults_before.insert(op.to_string(), Vec::new());
            crash_faults_after.insert(op.to_string(), Vec::new());
        }

        LazyFS {
            cache,
            config,
            faults,
            crash_faults: HashMap::new(),
            pending_write: Mutex::new(Write::default()),
            path_injecting_fault: Mutex::new(PathBuf::from("none")),

            crash_faults_before,
            crash_faults_after,

            allow_crash_fs_ops: [
                "unlink", "truncate", "fsync", "write", "create", "access", "open", "read",
                "rename", "link", "symlink",
            ]
            .iter()
            .map(|&s| s.into())
            .collect(),
            fs_op_mult_path: ["rename", "link", "symlink"]
                .iter()
                .map(|&s| s.into())
                .collect(),
        }
    }

    pub fn get_path_injecting_fault(&self) -> Result<PathBuf> {
        let lock = self
            .path_injecting_fault
            .lock()
            .map_err(|e| anyhow!("Unable to acquire lock on path injecting fault: {:?}", e))?;
        Ok(lock.clone())
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

impl Default for Write {
    fn default() -> Self {
        Write {
            path: "".into(),
            buf: Vec::new(),
            offset: 0,
        }
    }
}
