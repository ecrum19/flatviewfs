#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "Building rustdoc into docs/rustdoc ..."
cargo doc --no-deps --target-dir "$repo_root/target"
mkdir -p "$repo_root/docs/rustdoc"
rsync -a --delete "$repo_root/target/doc/" "$repo_root/docs/rustdoc/"

echo "Docs ready at docs/rustdoc/index.html (serve via GitHub Pages or open locally)."
