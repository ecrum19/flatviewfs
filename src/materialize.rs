use std::{collections::BTreeMap, path::PathBuf, sync::Arc, thread};

use anyhow::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use duckdb::Connection;

use crate::{cache::CacheEntry, duckdb_runner::run_job, route::CompiledRoute};

#[derive(Debug, Clone)]
pub struct MaterializeJob {
    pub route: CompiledRoute,
    pub params: BTreeMap<String, String>,
    pub files: Vec<PathBuf>,
    pub entry: Arc<CacheEntry>,
}

#[derive(Clone)]
pub struct MaterializerPool {
    tx: Sender<MaterializeJob>,
}

impl MaterializerPool {
    pub fn new(workers: usize) -> Self {
        let (tx, rx) = unbounded::<MaterializeJob>();

        for i in 0..workers.max(1) {
            let rx = rx.clone();
            thread::Builder::new()
                .name(format!("flatviewfs-worker-{i}"))
                .spawn(move || worker_loop(rx))
                .expect("spawn worker");
        }

        Self { tx }
    }

    pub fn submit(&self, job: MaterializeJob) -> Result<()> {
        self.tx.send(job)?;
        Ok(())
    }
}

fn worker_loop(rx: Receiver<MaterializeJob>) {
    let conn = Connection::open_in_memory().expect("open duckdb");
    crate::duckdb_runner::configure_connection(&conn).expect("configure duckdb");

    while let Ok(job) = rx.recv() {
        if let Err(err) = run_job(&conn, job.clone()) {
            job.entry.fail(err);
        }
    }
}
