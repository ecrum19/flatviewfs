use std::collections::HashMap;
use std::ffi::OsStr;
use std::process;
use std::path::PathBuf;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use clap::Parser;
use ctrlc;
use duckdb::types::Value;
use duckdb::Connection;
use fuser::FileAttr;
use fuser::FileType;
use fuser::Filesystem;
use fuser::MountOption;
use fuser::ReplyAttr;
use fuser::ReplyData;
use fuser::ReplyDirectory;
use fuser::ReplyEntry;
use fuser::Request;
use fuser::Session;
use fuser::FUSE_ROOT_ID;
use libc::ENOENT;

#[derive(Parser)]
#[command(version, author = "Christopher Berner")]
struct Args {
    /// Where to mount the CSV view
    pub mount_point: PathBuf,

    /// Allow root user to access filesystem
    #[clap(long)]
    pub allow_root: bool,

    /// Automatically unmount on process exit
    #[clap(long, default_value_t = true)]
    pub auto_unmount: bool,
}

const TTL: Duration = Duration::from_secs(1); // 1 second

#[derive(Clone)]
struct TableFile {
    ino: u64,
    name: String,
    content: String,
    attr: FileAttr,
}

struct DuckFuse {
    files: Vec<TableFile>,
}

impl DuckFuse {
    fn new() -> duckdb::Result<Self> {
        let mut conn = Connection::open_in_memory()?;
        seed_data(&mut conn)?;

        let mut files = Vec::new();
        let mut next_ino = 2; // 1 is root
        for (name, content) in render_tables_to_csv(&mut conn)? {
            let attr = file_attr(next_ino, content.len() as u64);
            files.push(TableFile {
                ino: next_ino,
                name,
                content,
                attr,
            });
            next_ino += 1;
        }

        Ok(Self { files })
    }

    fn lookup_by_name(&self, name: &OsStr) -> Option<&TableFile> {
        self.files
            .iter()
            .find(|f| f.name.as_str() == name.to_string_lossy())
    }

    fn lookup_by_ino(&self, ino: u64) -> Option<&TableFile> {
        self.files.iter().find(|f| f.ino == ino)
    }
}

impl Filesystem for DuckFuse {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        if let Some(file) = self.lookup_by_name(name) {
            reply.entry(&TTL, &file.attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == FUSE_ROOT_ID {
            let root_attr = dir_attr(self.files.len() as u32 + 2);
            reply.attr(&TTL, &root_attr);
            return;
        }

        if let Some(file) = self.lookup_by_ino(ino) {
            reply.attr(&TTL, &file.attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        if let Some(file) = self.lookup_by_ino(ino) {
            let bytes = file.content.as_bytes();
            let start = offset as usize;
            let end = usize::min(start + size as usize, bytes.len());
            if start >= bytes.len() {
                reply.data(&[]);
            } else {
                reply.data(&bytes[start..end]);
            }
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        let mut entries: Vec<(u64, FileType, String)> = vec![
            (FUSE_ROOT_ID, FileType::Directory, ".".into()),
            (FUSE_ROOT_ID, FileType::Directory, "..".into()),
        ];

        for file in &self.files {
            entries.push((file.ino, FileType::RegularFile, file.name.clone()));
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }
}

fn dir_attr(nlink: u32) -> FileAttr {
    FileAttr {
        ino: FUSE_ROOT_ID,
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o755,
        nlink,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn file_attr(ino: u64, size: u64) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: ((size + 511) / 512),
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::RegularFile,
        perm: 0o444,
        nlink: 1,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

fn seed_data(conn: &mut Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE ducks (id INTEGER, name TEXT);
        INSERT INTO ducks VALUES
            (1, 'Donald Duck'),
            (2, 'Scrooge McDuck'),
            (3, 'Darkwing Duck');

        CREATE TABLE ponds (id INTEGER, name TEXT, area_acres DOUBLE);
        INSERT INTO ponds VALUES
            (1, 'Mallard Marsh', 12.5),
            (2, 'Quacker Lagoon', 6.2);
        "#,
    )?;
    Ok(())
}

fn render_tables_to_csv(conn: &mut Connection) -> duckdb::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    map.insert("ducks.csv".into(), table_to_csv(conn, "SELECT * FROM ducks")?);
    map.insert("ponds.csv".into(), table_to_csv(conn, "SELECT * FROM ponds")?);
    Ok(map)
}

fn table_to_csv(conn: &mut Connection, sql: &str) -> duckdb::Result<String> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let stmt_ref = rows.as_ref().expect("statement missing");
    let col_names = (0..stmt_ref.column_count())
        .map(|i| stmt_ref.column_name(i).unwrap().clone())
        .collect::<Vec<_>>();
    let mut csv = String::new();
    csv.push_str(&col_names.join(","));
    csv.push('\n');

    while let Some(row) = rows.next()? {
        for (i, _name) in col_names.iter().enumerate() {
            if i > 0 {
                csv.push(',');
            }
            let val: Value = row.get(i)?;
            csv.push_str(&value_to_string(val));
        }
        csv.push('\n');
    }

    Ok(csv)
}

fn value_to_string(val: Value) -> String {
    match val {
        Value::Null => "".into(),
        Value::Boolean(v) => v.to_string(),
        Value::TinyInt(v) => v.to_string(),
        Value::SmallInt(v) => v.to_string(),
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::UTinyInt(v) => v.to_string(),
        Value::USmallInt(v) => v.to_string(),
        Value::UInt(v) => v.to_string(),
        Value::UBigInt(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Double(v) => v.to_string(),
        Value::Decimal(v) => v.to_string(),
        Value::Text(v) => v,
        other => format!("{:?}", other),
    }
}

fn main() {
    let args = Args::parse();
    env_logger::init();

    let mount_point = args.mount_point.clone();

    let mut cfg: Vec<MountOption> = Vec::new();
    if args.allow_root {
        cfg.push(MountOption::AllowRoot);
    }
    if args.auto_unmount {
        cfg.push(MountOption::AutoUnmount);
    }
    cfg.extend([MountOption::RO, MountOption::FSName("duckfuse".to_string())]);

    let fs = DuckFuse::new().expect("failed to initialize duckdb data");
    let mut session = Session::new(fs, &mount_point, &cfg).unwrap();

    let mut unmounter = session.unmount_callable();
    let mount_point_for_signal = mount_point.clone();
    ctrlc::set_handler(move || {
        if let Err(err) = unmounter.unmount() {
            eprintln!(
                "Failed to unmount {:?} after interrupt: {}",
                mount_point_for_signal,
                err
            );
        }
        process::exit(0);
    })
    .expect("failed to install Ctrl-C handler");

    session.run().unwrap();
}
