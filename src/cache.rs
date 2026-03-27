use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use parking_lot::{Condvar, Mutex};

#[derive(Debug)]
pub struct CacheManager {
    root: PathBuf,
    entries: Mutex<HashMap<String, Arc<CacheEntry>>>,
}

#[derive(Debug)]
pub struct CacheEntry {
    pub key: String,
    pub data_path: PathBuf,
    writer: Mutex<File>,
    progress: Mutex<Progress>,
    cv: Condvar,
    furthest_requested: AtomicU64,
}

#[derive(Debug, Clone)]
struct Progress {
    produced: u64,
    eof: bool,
    failed: Option<String>,
    chunks_ready: Vec<bool>,
}

pub const CHUNK_SIZE: u64 = 1 << 20; // 1 MiB
pub const PRODUCER_LEAD_CHUNKS: u64 = 4;
pub const PRODUCER_LEAD_TIMEOUT_MS: u64 = 200; // allow progress if no readers advance

impl CacheManager {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
            entries: Mutex::new(HashMap::new()),
        })
    }

    pub fn get_or_create(&self, key: &str) -> Result<(Arc<CacheEntry>, bool)> {
        if let Some(entry) = self.entries.lock().get(key).cloned() {
            if !entry.is_failed() {
                return Ok((entry, false));
            }
            self.entries.lock().remove(key);
        }

        let data_path = self.root.join(format!("{key}.data"));
        let writer = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&data_path)?;

        let entry = Arc::new(CacheEntry {
            key: key.to_string(),
            data_path,
            writer: Mutex::new(writer),
            progress: Mutex::new(Progress {
                produced: 0,
                eof: false,
                failed: None,
                chunks_ready: Vec::new(),
            }),
            cv: Condvar::new(),
            furthest_requested: AtomicU64::new(0),
        });

        self.entries.lock().insert(key.to_string(), entry.clone());
        Ok((entry, true))
    }

    pub fn get(&self, key: &str) -> Option<Arc<CacheEntry>> {
        self.entries.lock().get(key).cloned()
    }

    pub fn drop_entry(&self, key: &str) {
        self.entries.lock().remove(key);
    }
}

impl CacheEntry {
    pub fn is_failed(&self) -> bool {
        self.progress.lock().failed.is_some()
    }

    pub fn append(&self, bytes: &[u8]) -> Result<()> {
        if self.is_failed() {
            return Err(anyhow!("entry canceled or failed"));
        }
        {
            let mut file = self.writer.lock();
            file.write_all(bytes)?;
        }

        let mut p = self.progress.lock();
        p.produced += bytes.len() as u64;
        let needed_chunks = (p.produced + CHUNK_SIZE - 1) / CHUNK_SIZE;
        if p.chunks_ready.len() < needed_chunks as usize {
            p.chunks_ready.resize(needed_chunks as usize, false);
        }
        for idx in 0..p.chunks_ready.len() {
            let chunk_end = ((idx as u64 + 1) * CHUNK_SIZE).min(p.produced);
            if chunk_end >= (idx as u64 + 1) * CHUNK_SIZE {
                p.chunks_ready[idx] = true;
            }
        }
        self.cv.notify_all();
        Ok(())
    }

    pub fn finish(&self) {
        let mut p = self.progress.lock();
        p.eof = true;
        if p.produced > 0 {
            let needed_chunks = (p.produced + CHUNK_SIZE - 1) / CHUNK_SIZE;
            if p.chunks_ready.len() < needed_chunks as usize {
                p.chunks_ready.resize(needed_chunks as usize, true);
            }
        }
        self.cv.notify_all();
    }

    pub fn fail(&self, err: anyhow::Error) {
        let mut p = self.progress.lock();
        p.failed = Some(format!("{err:#}"));
        p.eof = true;
        self.cv.notify_all();
    }

    pub fn cancel(&self, reason: &str) {
        let mut p = self.progress.lock();
        if p.eof {
            return;
        }
        p.failed = Some(reason.to_string());
        p.eof = true;
        self.cv.notify_all();
    }

    pub fn size_hint(&self) -> u64 {
        self.progress.lock().produced
    }

    pub fn is_complete(&self) -> bool {
        let p = self.progress.lock();
        p.eof && p.failed.is_none()
    }

    pub fn request_offset(&self, offset: u64) {
        loop {
            let current = self.furthest_requested.load(Ordering::Relaxed);
            if offset <= current {
                break;
            }
            if self
                .furthest_requested
                .compare_exchange(current, offset, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        self.cv.notify_all();
    }

    fn chunk_ready(p: &Progress, idx: usize) -> bool {
        p.chunks_ready.get(idx).copied().unwrap_or(false)
            || (p.eof && (idx as u64) * CHUNK_SIZE < p.produced)
    }

    pub fn wait_for_need(&self, produced: u64) -> Result<()> {
        use std::time::Duration;
        let mut p = self.progress.lock();
        loop {
            if let Some(err) = &p.failed {
                return Err(anyhow!(err.clone()));
            }
            if p.eof {
                return Ok(());
            }
            let furthest = self.furthest_requested.load(Ordering::Relaxed);
            let target = furthest
                .saturating_add(PRODUCER_LEAD_CHUNKS.saturating_mul(CHUNK_SIZE))
                .max(PRODUCER_LEAD_CHUNKS.saturating_mul(CHUNK_SIZE));
            if produced < target {
                return Ok(());
            }
            let timeout = Duration::from_millis(PRODUCER_LEAD_TIMEOUT_MS);
            let result = self.cv.wait_for(&mut p, timeout);
            if result.timed_out() {
                // No reader advanced within timeout; allow producer to continue.
                return Ok(());
            }
        }
    }

    pub fn read_blocking_at(&self, reader: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.request_offset(offset + len as u64);
        let target_chunk = (offset / CHUNK_SIZE) as usize;
        loop {
            let mut p = self.progress.lock();

            if let Some(err) = &p.failed {
                return Err(anyhow!(err.clone()));
            }

            if Self::chunk_ready(&p, target_chunk) || offset < p.produced || p.eof {
                let available = (p.produced.saturating_sub(offset)) as usize;
                let n = available.min(len);
                drop(p);

                let mut buf = vec![0; n];
                let mut read = 0;
                while read < n {
                    let nread = reader.read_at(&mut buf[read..], offset + read as u64)?;
                    if nread == 0 {
                        return Err(anyhow!("unexpected EOF at offset {}", offset + read as u64));
                    }
                    read += nread;
                }
                return Ok(buf);
            }

            if p.eof {
                return Ok(Vec::new());
            }

            self.cv.wait(&mut p);
        }
    }

    pub fn read_blocking(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let reader = File::open(&self.data_path)?;
        self.read_blocking_at(&reader, offset, len)
    }
}
