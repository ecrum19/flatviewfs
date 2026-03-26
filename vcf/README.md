# VCF use-case


## VCF-to-TSV Canonical Split Schema

## Summary
A canonical, round-trip-safe TSV export that treats the VCF as a normalized relational package rather than a single flattened table. The export preserves enough structure to reconstruct a standards-compliant VCF exactly in field meaning and record/sample ordering, while also loading efficiently into DuckDB.

Chosen defaults:
- Optimize for round-trip fidelity first, DuckDB second.
- Represent genotype/sample data with one TSV per unique ordered `FORMAT` signature.
- Keep a canonical normalized output; optional convenience views/tables can be derived later, not treated as source-of-truth.


## Public Interfaces / Conventions
- CLI contract for the splitter:
  - Input: single `.vcf`/`.vcf.gz` file or directory
  - Output: one subdirectory per input VCF containing the canonical TSV package
  - Deterministic file naming for genotype tables: `genotype_sig_<n>.tsv`, with lookup in `format_signatures.tsv`
- Schema versioning:
  - Include `schema_version` in the manifest from day one
  - Treat schema changes as versioned, never implicit
- Header parsing rules:
  - Raw header lines are authoritative
  - Parsed header definitions are derived, normalized helpers
  - If a header line cannot be structurally parsed, keep it in `header_lines.tsv` and mark parsed fields as null rather than dropping it

## Test Plan
- Single-sample VCF with fixed `FORMAT`
- Multi-sample VCF with shared `FORMAT`
- Mixed `FORMAT` signatures across records, producing multiple genotype TSVs
- Records with `INFO` flags, scalar values, and comma-separated lists
- Records with multi-allelic `ALT`
- Records with `FILTER=PASS`, `FILTER=.`, and multiple filter values
- Missing genotype subfields and partially populated sample entries
- Header definitions for `INFO`, `FORMAT`, `FILTER`, `ALT`, and `contig`
- Round-trip test: VCF -> TSV package -> reconstructed VCF, then semantic diff against original
- DuckDB load test: ingest all canonical TSVs and confirm joins on `record_id`, `sample_id`, and `format_signature_id`

## Assumptions
- The canonical package is the source of truth; any denormalized analytics tables are downstream derivatives.
- One concept maps to one column only in the canonical layer; repeated/list values are represented as repeated rows plus ordinal columns, not packed strings.
- Per-signature genotype tables remain generalizable because the signature registry is explicit and deterministic.
- Exact byte-for-byte reconstruction is not required if cosmetic whitespace differs, but semantic reconstruction of a valid equivalent VCF is required.
