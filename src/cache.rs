use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, anyhow};
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
            return Ok((entry, false));
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
}

impl CacheEntry {
    pub fn append(&self, bytes: &[u8]) -> Result<()> {
        {
            let mut file = self.writer.lock();
            file.write_all(bytes)?;
            file.flush()?;
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

    pub fn size_hint(&self) -> u64 {
        self.progress.lock().produced
    }

    pub fn is_complete(&self) -> bool {
        let p = self.progress.lock();
        p.eof && p.failed.is_none()
    }

    pub fn read_blocking(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        loop {
            let mut p = self.progress.lock();

            if let Some(err) = &p.failed {
                return Err(anyhow!(err.clone()));
            }

            if offset < p.produced {
                let available = (p.produced - offset) as usize;
                let n = available.min(len);
                drop(p);

                let mut f = File::open(&self.data_path)?;
                f.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0; n];
                f.read_exact(&mut buf)?;
                return Ok(buf);
            }

            if p.eof {
                return Ok(Vec::new());
            }

            self.cv.wait(&mut p);
        }
    }
}
