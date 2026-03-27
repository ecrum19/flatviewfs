# Contributing

## Expectations

- **Safety over convenience**: use prepared statements for user input; keep path literals explicit for DuckDB table functions.
- **Formatter isolation**: keep formatter-specific logic in `src/formatters/*` and manifests; avoid special cases in the FUSE layer.
- **Tests**: add integration tests under `tests/` when changing manifests or formatters. Run `cargo test -- --nocapture` before sending changes.
- **Docs**: update `PLAN.md` for substantive work; log new benchmarks in `BENCHMARK.md`.

## Coding style

- Rust 2021 edition; prefer idiomatic ownership and error handling via `anyhow`.
- Avoid unnecessary flushing; batch writes where possible.
- Keep comments concise and only when they add clarity.

## Adding routes

- Declare `package_root` explicitly in manifests; list sidecar TSV/Parquet inputs in `extra_inputs` so cache keys stay correct.
- Keep paths and SQL templates in sync; avoid inferred paths when possible.

## Dynamic directories

- Current discovery is minimal (flat paths from source globs). If you extend dynamic listing, keep it deterministic and cheap in `getattr`/`readdir`.

## Rust docs and GitHub Pages

- Generate API docs with `./scripts/build-docs.sh`. Do not commit `docs/rustdoc` unless publishing pages.

## Review checklist

- Are cache keys correct for all inputs?
- Are new files discoverable (readdir) and readable?
- Do cancellation and producer lead semantics handle short reads?
- Tests and benchmarks updated?
