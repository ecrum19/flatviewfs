# flatviewfs

Phase-1 proof-of-concept / MVP for exposing a query + transformation over Parquet on disk as a plain text file in FUSE, using DuckDB under the hood.

What is included:

- static manifest-driven routes
- one route pattern example: `/samples/{sample}.vcf`
- DuckDB-backed materialization into a local spool file
- shared cache per snapshot key
- blocking reads for incomplete materializations
- a VCF formatter skeleton

Notes:

- This is a skeleton, not a finished production crate.
- The code is structured to make the FUSE adapter, materializer, and formatter easy to iterate on.

## Layout

- `examples/vcf-manifest.toml`: example route manifest
- `src/main.rs`: mount entrypoint
- `src/fs.rs`: FUSE filesystem
- `src/materialize.rs`: worker pool and job dispatch
- `src/duckdb_runner.rs`: DuckDB query execution
- `src/cache.rs`: spool-file backed shared cache
- `src/formatters/vcf.rs`: VCF text formatter
- `src/route.rs`: route compilation and path matching

## Next steps

1. Replace string-substituted scalar SQL injection with prepared statements.
2. Persist cache metadata and chunk index across restarts.
3. Improve directory enumeration for dynamic paths.
4. Add integration tests and compile-fix against the exact `fuser` and `duckdb` versions you choose.
5. Add BGZF + tabix sidecar support for genomic region-aware workflows.

## Tiny smoke tests

- `cargo test materialize -- --nocapture` runs two integration tests:
  - `materializes_csv_small_example` materializes a two-row table to CSV output (example data in `examples/data/tiny.csv`).
  - `materializes_vcf_small_example` materializes a single-record VCF file end-to-end via DuckDB + formatter.
