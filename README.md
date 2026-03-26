# flatviewfs

Mount query results as read-only files via FUSE, materializing on demand with DuckDB.

## What works now

- Canonical VCF packages produced by `vcf/canonical_tsv.py` can be reconstructed back into VCF text through a manifest plus the VCF formatter.
- CSV formatter for simple tabular outputs.
- Prepared-statement binding for user parameters to avoid SQL injection of scalars.

## Usage sketch

1) Generate canonical packages (Parquet/TSV) from VCFs:

   ```bash
   python vcf/canonical_tsv.py split vcf/tests/test-files/0GOOR_HG002_subset.vcf vcf/generated
   ```
2) Point the manifest at the generated package, e.g. `examples/vcf-canonical.toml`.
3) Mount:

   ```bash
   cargo run -- --manifest examples/vcf-canonical.toml --mountpoint /tmp/flatviewfs --cache-dir /var/tmp/flatviewfs
   ```
   Access files like `/tmp/flatviewfs/vcf/0GOOR_HG002_subset/HG002_PacBio_Clc_OTS_PASS_Hg38_no_alt.vcf`.

## Testing
- `cargo test -- --nocapture` runs:
  - CSV materialization smoke test.
  - VCF materialization smoke test (synthetic mini package).
  - Canonical end-to-end reconstruction test against the generated package for `0GOOR_HG002_subset`.

## Notes
- DuckDB table functions require literal file paths; manifests substitute package paths as literals while user inputs are parameter-bound.
- INFO/FORMAT reconstruction depends on the canonical TSV schema emitted by `canonical_tsv.py`; schema changes will require manifest/formatter updates.
