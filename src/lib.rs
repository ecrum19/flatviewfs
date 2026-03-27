//! flatviewfs — mount DuckDB query results as read-only files via FUSE.
//!
//! Design goals:
//! - Manifest-driven routing from virtual paths to Parquet/TSV inputs and SQL.
//! - Stream materialization: DuckDB Arrow batches → formatter → chunked cache → FUSE.
//! - Safety: prepared statements for user input; explicit literals for paths; read-only FS.
//!
//! Key modules:
//! - `manifest` / `route`: parse and render manifests, match virtual paths, fingerprint snapshots.
//! - `materialize` / `duckdb_runner`: worker pool and DuckDB streaming execution.
//! - `cache`: chunked spool files with per-chunk readiness, producer lead, cancellation.
//! - `formatters`: CSV and VCF renderers with lazy sidecar loading for VCF packages.
//! - `fs`: FUSE glue; handles lookup, open, read, and dynamic discovery of flat routes.
//!
//! For usage, see `docs/usage.md` and the example manifests under `examples/`.
pub mod cache;
pub mod duckdb_runner;
pub mod formatters;
pub mod fs;
pub mod manifest;
pub mod materialize;
pub mod route;
