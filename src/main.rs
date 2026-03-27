use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use flatviewfs::{fs::ParqFs, manifest::Manifest};
use fuser::MountOption;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,

    #[arg(long)]
    mountpoint: PathBuf,

    #[arg(long, default_value = "/var/tmp/flatviewfs")]
    cache_dir: PathBuf,

    #[arg(long, default_value_t = 2)]
    workers: usize,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("flatviewfs=info")
        .init();

    let args = Args::parse();
    let manifest = Manifest::load(&args.manifest)?;
    let fs = ParqFs::new(manifest, args.cache_dir, args.workers)?;

    let options: [MountOption; 0] = [];
    fuser::mount2(fs, &args.mountpoint, &options)?;

    Ok(())
}
