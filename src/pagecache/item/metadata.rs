use std::time::SystemTime;

#[derive(Debug)]
pub struct Metadata {
    pub nlinks: u32,
    pub size: i32,
    pub atim: SystemTime,
    pub mtim: SystemTime,
    pub ctim: SystemTime,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            nlinks: 0,
            size: 0,
            atim: SystemTime::now(),
            mtim: SystemTime::now(),
            ctim: SystemTime::now(),
        }
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            nlinks: 1,
            size: 0,
            atim: SystemTime::now(),
            mtim: SystemTime::now(),
            ctim: SystemTime::now(),
        }
    }
}
