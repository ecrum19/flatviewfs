# Architecture

## High-level shape

- **Manifest-driven routing** (`manifest.rs`, `route.rs`): TOML manifests define virtual paths, data sources, and SQL. Each route has a path template, source glob, optional package root, and formatter.
- **Materialization pipeline** (`fs.rs`, `materialize.rs`, `duckdb_runner.rs`): Opening a virtual file resolves a snapshot key, gets/creates a cache entry, and submits a DuckDB job. The job streams Arrow batches, formats them, and appends bytes into the cache.
- **Cache** (`cache.rs`): Active entries live under a spool dir. Writes are chunked (1 MiB) with per-chunk readiness, a furthest-request hint, and optional cancellation when the last handle closes.
- **Formatters** (`formatters/`): CSV and VCF formatters turn Arrow batches into text. VCF pulls canonical sidecar TSVs lazily (per signature/sample) via paths rooted at `package_root`.
- **FUSE layer** (`fs.rs`): Read-only FUSE FS that maps virtual paths to cache entries. Uses `DIRECT_IO` for active files and `KEEP_CACHE` when complete. Includes basic dynamic discovery to surface flat routes in `readdir`.

## Key data flows

1. **Path resolution**: `CompiledRoute::match_path` matches a virtual path to a route and params. `resolve_files` globs the `source_glob` and any `extra_inputs`, applying `package_root` if present. `snapshot_key` fingerprints SQL, params, and inputs.
2. **Materialization**: `MaterializerPool` threads share an in-memory DuckDB. `run_job` renders SQL (injecting package root), streams Arrow with `stream_arrow`, and hands batches to a formatter.
3. **Formatting**: Formatters buffer per batch. VCF lazily loads signature TSVs and genotype/info rows by signature to avoid huge upfront cost.
4. **Cache & IO**: `CacheEntry::append` updates produced byte count and chunk readiness, waking readers. Readers block on chunk readiness or EOF. Producer waits if it is more than four chunks ahead, with a timeout to avoid deadlock when no readers advance.
5. **FUSE reads**: `open` attaches a shared file handle and chooses `DIRECT_IO` vs `KEEP_CACHE`. `read` uses `pread` via `read_blocking_at` to avoid reopening files.

## Current constraints

- Dynamic directories are minimal; only literal prefixes plus discovered params are populated. Deep hierarchies and full dynamic listings are still TBD.
- Cache persistence across restarts is not implemented; chunk metadata is in-memory only.
- Cache keys rely on manifests listing sidecar inputs (`extra_inputs`); missing inputs can lead to stale cache.
- Package roots are explicit but not enforced; legacy manifests may still omit them.
