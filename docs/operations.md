# Operations

## Running the daemon

- Use release builds for real workloads: `target/release/flatviewfs ...`.
- Set `--cache-dir` to a fast local disk (e.g., `/var/tmp/flatviewfs`).
- Choose worker count based on CPU and DuckDB resource limits; default is 2.

## Cancellation and lead

- Active entries use a 4 MiB producer lead; the writer pauses if far ahead of readers but resumes after a short timeout to avoid deadlock.
- When the last handle closes, in-progress entries are canceled and evicted unless `FLATVIEWFS_DISABLE_CANCEL=1` is set.

## Troubleshooting

- **Socket is not connected**: unmount stale mounts (`umount /tmp/flatviewfs`).
- **Slow first-byte latency**: ensure manifests list `extra_inputs`, package roots are correct, and prefer flat-path manifests to avoid dynamic-dir gaps.
- **High memory**: remember DuckDB connections default to `threads=4`, `memory_limit='4GB'` per worker.

