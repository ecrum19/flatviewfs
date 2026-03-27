use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ffi::OsStr,
    fs::File,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use anyhow::Result;
use fuser::{
    consts::{FOPEN_DIRECT_IO, FOPEN_KEEP_CACHE},
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, Request,
};
use parking_lot::RwLock;

use crate::{
    cache::{CacheEntry, CacheManager},
    manifest::Manifest,
    materialize::{MaterializeJob, MaterializerPool},
    route::{path_parent, CompiledManifest},
};

const TTL: Duration = Duration::from_secs(1);
const ROOT_INO: u64 = 1;

#[derive(Debug, Clone)]
enum Node {
    Dir {
        path: String,
    },
    File {
        path: String,
        route_index: usize,
        params: BTreeMap<String, String>,
    },
}

#[derive(Debug)]
struct FsIndex {
    by_ino: HashMap<u64, Node>,
    by_path: HashMap<String, u64>,
    next_ino: u64,
}

struct OpenHandle {
    entry: Arc<CacheEntry>,
    reader: File,
}

pub struct ParqFs {
    routes: CompiledManifest,
    cache: CacheManager,
    materializers: MaterializerPool,
    index: RwLock<FsIndex>,
    open_handles: RwLock<HashMap<u64, Arc<OpenHandle>>>,
    next_fh: AtomicU64,
}

impl ParqFs {
    pub fn new(manifest: Manifest, cache_dir: std::path::PathBuf, workers: usize) -> Result<Self> {
        let routes = CompiledManifest::compile(manifest)?;
        let mut by_ino = HashMap::new();
        let mut by_path = HashMap::new();

        by_ino.insert(ROOT_INO, Node::Dir { path: "/".into() });
        by_path.insert("/".into(), ROOT_INO);

        let mut next_ino = ROOT_INO + 1;
        for dir in &routes.static_dirs {
            if dir == "/" {
                continue;
            }
            by_ino.insert(next_ino, Node::Dir { path: dir.clone() });
            by_path.insert(dir.clone(), next_ino);
            next_ino += 1;
        }

        Ok(Self {
            routes,
            cache: CacheManager::new(cache_dir)?,
            materializers: MaterializerPool::new(workers),
            index: RwLock::new(FsIndex {
                by_ino,
                by_path,
                next_ino,
            }),
            open_handles: RwLock::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        })
    }

    fn ensure_file_node(&self, full_path: &str) -> Option<u64> {
        if let Some(ino) = self.index.read().by_path.get(full_path).copied() {
            return Some(ino);
        }

        let matched = self.routes.match_path(full_path)?;
        let mut idx = self.index.write();
        if let Some(ino) = idx.by_path.get(full_path).copied() {
            return Some(ino);
        }

        let ino = idx.next_ino;
        idx.next_ino += 1;
        idx.by_path.insert(full_path.to_string(), ino);
        idx.by_ino.insert(
            ino,
            Node::File {
                path: full_path.to_string(),
                route_index: matched.route_index,
                params: matched.params,
            },
        );
        Some(ino)
    }

    fn node(&self, ino: u64) -> Option<Node> {
        self.index.read().by_ino.get(&ino).cloned()
    }

    fn child_names(&self, dir_path: &str) -> BTreeSet<String> {
        let idx = self.index.read();
        idx.by_path
            .keys()
            .filter(|path| *path != dir_path)
            .filter(|path| path_parent(path) == dir_path)
            .filter_map(|path| Path::new(path).file_name().and_then(|x| x.to_str()))
            .map(|x| x.to_string())
            .collect()
    }

    fn attr_for(&self, ino: u64, node: &Node) -> FileAttr {
        let now = SystemTime::now();
        let (kind, perm, nlink, size) = match node {
            Node::Dir { .. } => (FileType::Directory, 0o555, 2, 0),
            Node::File {
                path,
                route_index,
                params,
            } => {
                let route = &self.routes.routes[*route_index];
                let size = route
                    .resolve_files(params)
                    .ok()
                    .and_then(|files| route.snapshot_key(params, &files).ok())
                    .and_then(|k| self.cache.get(&k))
                    .map(|e| e.size_hint())
                    .unwrap_or(0);
                let _ = path;
                (FileType::RegularFile, 0o444, 1, size)
            }
        };

        FileAttr {
            ino: ino.into(),
            size,
            blocks: size.div_ceil(512),
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind,
            perm,
            nlink,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn open_virtual_file(
        &self,
        route_index: usize,
        params: &BTreeMap<String, String>,
    ) -> Result<Arc<CacheEntry>> {
        let route = self.routes.routes[route_index].clone();
        let files = route.resolve_files(params)?;
        let key = route.snapshot_key(params, &files)?;
        let (entry, created) = self.cache.get_or_create(&key)?;
        if created {
            self.materializers.submit(MaterializeJob {
                route,
                params: params.clone(),
                files,
                entry: entry.clone(),
            })?;
        }
        Ok(entry)
    }
}

impl Filesystem for ParqFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(Node::Dir { path: parent_path }) = self.node(parent) else {
            reply.error(libc::ENOENT);
            return;
        };

        let full_path = if parent_path == "/" {
            format!("/{}", name.to_string_lossy())
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };

        let existing = { self.index.read().by_path.get(&full_path).copied() };
        let ino = existing.or_else(|| self.ensure_file_node(&full_path));

        let Some(ino) = ino else {
            reply.error(libc::ENOENT);
            return;
        };

        let node = self.node(ino).unwrap();
        let attr = self.attr_for(ino, &node);
        reply.entry(&TTL, &attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let Some(node) = self.node(ino) else {
            reply.error(libc::ENOENT);
            return;
        };

        let attr = self.attr_for(ino, &node);
        reply.attr(&TTL, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(Node::Dir { path }) = self.node(ino) else {
            reply.error(libc::ENOTDIR);
            return;
        };

        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (
                if path == "/" {
                    ino
                } else {
                    self.index
                        .read()
                        .by_path
                        .get(path_parent(&path))
                        .copied()
                        .unwrap_or(ROOT_INO)
                },
                FileType::Directory,
                "..".into(),
            ),
        ];

        for name in self.child_names(&path) {
            let child_path = if path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", path, name)
            };
            if let Some(child_ino) = self.index.read().by_path.get(&child_path).copied() {
                let ty = match self.node(child_ino).unwrap() {
                    Node::Dir { .. } => FileType::Directory,
                    Node::File { .. } => FileType::RegularFile,
                };
                entries.push((child_ino, ty, name));
            }
        }

        for (i, (e_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(e_ino, (i + 1) as i64, kind, name) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let Some(Node::File {
            route_index,
            params,
            ..
        }) = self.node(ino)
        else {
            reply.error(libc::EISDIR);
            return;
        };

        match self.open_virtual_file(route_index, &params) {
            Ok(entry) => {
                let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
                let reader = match File::open(&entry.data_path) {
                    Ok(f) => f,
                    Err(err) => {
                        tracing::error!("open reader failed: {err:#}");
                        reply.error(libc::EIO);
                        return;
                    }
                };
                let handle = Arc::new(OpenHandle {
                    entry: entry.clone(),
                    reader,
                });
                self.open_handles.write().insert(fh, handle);

                let flags = if entry.is_complete() {
                    FOPEN_KEEP_CACHE
                } else {
                    FOPEN_DIRECT_IO
                };
                reply.opened(fh, flags);
            }
            Err(err) => {
                tracing::error!("open failed: {err:#}");
                reply.error(libc::EIO);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(handle) = self.open_handles.read().get(&fh).cloned() else {
            reply.error(libc::EBADF);
            return;
        };

        match handle
            .entry
            .read_blocking_at(&handle.reader, offset as u64, size as usize)
        {
            Ok(bytes) => reply.data(&bytes),
            Err(err) => {
                tracing::error!("read failed: {err:#}");
                reply.error(libc::EIO);
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let handle = self.open_handles.write().remove(&fh);
        if let Some(handle) = handle {
            let still_open = self
                .open_handles
                .read()
                .values()
                .any(|h| h.entry.key == handle.entry.key);
            let cancel_disabled = std::env::var("FLATVIEWFS_DISABLE_CANCEL").is_ok();
            if !still_open && !handle.entry.is_complete() && !cancel_disabled {
                handle
                    .entry
                    .cancel("materialization canceled because last handle closed");
                self.cache.drop_entry(&handle.entry.key);
            }
        }
        reply.ok();
    }
}
