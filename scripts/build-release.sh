#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SCCACHE_DIR="${SCCACHE_DIR:-$root/.sccache}"
mkdir -p "$SCCACHE_DIR"

require() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing $1 (on Debian/Ubuntu: apt install $2)" >&2
        exit 1
    fi
}

require sccache sccache
require mold mold
require clang clang

cd "$root"
sccache --show-stats || true
cargo build --release "$@"
sccache --show-stats
