# Usage

## Build

```bash
cargo build            # debug
cargo build --release  # optimized, recommended for benchmarks
```

## Mounting

```bash
# choose a manifest and mountpoint
target/release/flatviewfs \
  --manifest examples/vcf-canonical-full-flat.toml \
  --mountpoint /tmp/flatviewfs \
  --cache-dir /var/tmp/flatviewfs \
  --workers 2
```

Unmount with `umount /tmp/flatviewfs` (macOS: `umount` on `/private/tmp/flatviewfs`). If a crash occurs, unmount before remounting to avoid stale sockets.

## Reading

Use normal tools: `head`, `cat`, `awk`, etc. Reads block until the needed chunk is ready. Active files use `DIRECT_IO`; completed files use `KEEP_CACHE`.

## Benchmarks

Example cold benchmark for full HG002:

```bash
/usr/bin/time -l head -n 1000 \
  /tmp/flatviewfs/vcf-0GOOR_HG002-HG002_PacBio_Clc_OTS_PASS_Hg38_no_alt.vcf
```

Set `FLATVIEWFS_DISABLE_CANCEL=1` if you want producers to finish even after readers close (useful for warming the cache once).

## Cache directory

The cache dir holds per-entry `.data` files. It is safe to delete between runs. Persistence across restarts is not implemented yet.

## Dynamic discovery

Flat VCF manifests (`*flat*.toml`) use parameter discovery from source globs to populate entries in `readdir`. Deeper dynamic hierarchies are not yet implemented.

## Rust API docs

Generate API docs into `docs/rustdoc` (for local browsing or GitHub Pages):

```bash
./scripts/build-docs.sh
open docs/rustdoc/flatviewfs/index.html
```
