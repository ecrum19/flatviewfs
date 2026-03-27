use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
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
}

#[derive(Debug, Clone)]
struct Progress {
    produced: u64,
    eof: bool,
    failed: Option<String>,
}

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
            }),
            cv: Condvar::new(),
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
        self.cv.notify_all();
        Ok(())
    }

    pub fn finish(&self) {
        let mut p = self.progress.lock();
        p.eof = true;
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

    pub fn read_blocking_at(&self, reader: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
        loop {
            let mut p = self.progress.lock();

            if let Some(err) = &p.failed {
                return Err(anyhow!(err.clone()));
            }

            if offset < p.produced {
                let available = (p.produced - offset) as usize;
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
