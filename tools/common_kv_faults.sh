#!/usr/bin/env bash
set -Eeuo pipefail

# Exercise deterministic process-termination boundaries for the common ordered-KV
# adapters. This is a portable recovery gate, not a substitute for block-layer
# power-loss testing: the child is killed while it is held before a batch commit.

usage() {
    cat >&2 <<'EOF'
usage: tools/common_kv_faults.sh [options]

Options:
  --output-dir PATH       Fresh directory for databases, logs, and manifest
                          (default: /tmp/seerdb-common-kv-faults-TIMESTAMP)
  --keys N                Key-space size (default: 256)
  --operations N          Operation count (default: 128)
  --batch-size N          Batch size (default: 16)
  --value-bytes N         Value size (default: 64)
  --seed N                Deterministic trace seed (default: 7)

The harness runs seerdb, fjall, and rocksdb. For each engine it kills one child
before batch 64 (old state) and one before batch 80 (complete-new state), then
verifies the expected prefix across two reopens.
EOF
}

die() {
    echo "common_kv_faults: $*" >&2
    exit 2
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${SEERDB_COMMON_KV_FAULTS_OUTPUT:-"/tmp/seerdb-common-kv-faults-$(date -u +%Y%m%dT%H%M%SZ)"}
keys=256
operations=128
batch_size=16
value_bytes=64
seed=7

while (($#)); do
    case "$1" in
        --output-dir) output_dir=${2:?missing value for --output-dir}; shift 2 ;;
        --keys) keys=${2:?missing value for --keys}; shift 2 ;;
        --operations) operations=${2:?missing value for --operations}; shift 2 ;;
        --batch-size) batch_size=${2:?missing value for --batch-size}; shift 2 ;;
        --value-bytes) value_bytes=${2:?missing value for --value-bytes}; shift 2 ;;
        --seed) seed=${2:?missing value for --seed}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) usage; die "unknown argument: $1" ;;
    esac
done

[[ $(uname -s) == Linux ]] || die "requires Linux for process-termination qualification"
for command in cargo git python3 uname; do
    command -v "$command" >/dev/null || die "missing required command: $command"
done

is_positive_integer() {
    [[ $1 =~ ^[1-9][0-9]*$ ]]
}

[[ $keys =~ ^[1-9][0-9]*$ ]] || die "--keys must be a positive integer"
[[ $operations =~ ^[1-9][0-9]*$ ]] || die "--operations must be a positive integer"
[[ $batch_size =~ ^[1-9][0-9]*$ ]] || die "--batch-size must be a positive integer"
[[ $value_bytes =~ ^[1-9][0-9]*$ ]] || die "--value-bytes must be a positive integer"
[[ $seed =~ ^[0-9]+$ ]] || die "--seed must be a non-negative integer"
((operations >= 80)) || die "--operations must reach the complete-new prefix 80"
((batch_size > 0 && 64 % batch_size == 0 && 80 % batch_size == 0)) || \
    die "--batch-size must divide both recovery prefixes 64 and 80"

[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir/runs" "$output_dir/db"

build_root=$(mktemp -d "${TMPDIR:-/tmp}/seerdb-common-kv-faults-build.XXXXXX")
cleanup() {
    local exit_code=$?
    rm -rf -- "$build_root"
    exit "$exit_code"
}
trap cleanup EXIT

bench_manifest="$repo_root/tools/common_kv_bench/Cargo.toml"
bench_name=seerdb-common-kv-bench

build_engine() {
    local engine=$1
    local target_dir="$build_root/$engine"
    local binary="$target_dir/release/$bench_name"
    case "$engine" in
        seerdb|fjall)
            if ! cargo build --release --manifest-path "$bench_manifest" --target-dir "$target_dir" >&2; then
                echo "common_kv_faults: failed to build $engine" >&2
                return 1
            fi
            ;;
        rocksdb)
            if ! cargo build --release --manifest-path "$bench_manifest" --target-dir "$target_dir" \
                --no-default-features --features rocksdb >&2; then
                echo "common_kv_faults: failed to build $engine" >&2
                return 1
            fi
            ;;
        *)
            echo "common_kv_faults: unknown engine $engine" >&2
            return 1
    esac
    if [[ ! -x "$binary" ]]; then
        echo "common_kv_faults: build succeeded but binary is missing: $binary" >&2
        return 1
    fi
    echo "$binary"
}

for engine in seerdb fjall rocksdb; do
    echo "common_kv_faults: building $engine" >&2
    built_binary=$(build_engine "$engine")
    case "$engine" in
        seerdb) seerdb_binary=$built_binary ;;
        fjall) fjall_binary=$built_binary ;;
        rocksdb) rocksdb_binary=$built_binary ;;
    esac
done

binary_for_engine() {
    case "$1" in
        seerdb) echo "$seerdb_binary" ;;
        fjall) echo "$fjall_binary" ;;
        rocksdb) echo "$rocksdb_binary" ;;
        *) die "unknown engine: $1" ;;
    esac
}

wait_for_boundary() {
    local pid=$1
    local marker=$2
    local expected=$3
    local attempt
    for ((attempt = 0; attempt < 6000; attempt++)); do
        if [[ -f "$marker" ]] && [[ $(tr -d '[:space:]' <"$marker") == "$expected" ]]; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        sleep 0.01
    done
    return 1
}

run_case() {
    local engine=$1
    local case_name=$2
    local prefix=$3
    local binary
    binary=$(binary_for_engine "$engine")
    local label="$engine.$case_name"
    local db_path="$output_dir/db/$label"
    local run_dir="$output_dir/runs/$label"
    local progress="$run_dir/progress"
    local hold="$run_dir/release"
    local verify_json="$run_dir/verify.json"
    local child_stdout="$run_dir/child.stdout"
    local child_stderr="$run_dir/child.stderr"

    mkdir -p "$run_dir"
    echo "common_kv_faults: kill $label before batch $prefix" >&2
    "$binary" \
        --engine "$engine" \
        --workload batch-put \
        --durability durable \
        --path "$db_path" \
        --keys "$keys" \
        --operations "$operations" \
        --batch-size "$batch_size" \
        --value-bytes "$value_bytes" \
        --range-width 1 \
        --seed "$seed" \
        --progress "$progress" \
        --progress-hold "$hold" \
        --progress-hold-index "$prefix" \
        >"$child_stdout" 2>"$child_stderr" &
    local child_pid=$!

    if ! wait_for_boundary "$child_pid" "$progress" "$prefix"; then
        kill -KILL "$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
        die "child exited or missed boundary $prefix for $label; see $child_stderr"
    fi

    if ! kill -KILL "$child_pid" 2>/dev/null; then
        wait "$child_pid" 2>/dev/null || true
        die "could not SIGKILL child $child_pid for $label"
    fi
    set +e
    wait "$child_pid"
    local child_status=$?
    set -e
    [[ $child_status -eq 137 ]] || die "expected SIGKILL status 137 for $label, got $child_status"
    printf '%s\n' "$child_status" >"$run_dir/child.status"

    "$binary" \
        --engine "$engine" \
        --workload batch-put \
        --durability durable \
        --path "$db_path" \
        --keys "$keys" \
        --operations "$operations" \
        --batch-size "$batch_size" \
        --value-bytes "$value_bytes" \
        --range-width 1 \
        --seed "$seed" \
        --verify-prefix "$prefix" \
        --output "$verify_json" \
        >"$run_dir/verify.stdout" 2>"$run_dir/verify.stderr"
}

run_case seerdb old-state 64
run_case seerdb complete-new-state 80
run_case fjall old-state 64
run_case fjall complete-new-state 80
run_case rocksdb old-state 64
run_case rocksdb complete-new-state 80

python3 - "$output_dir" "$repo_root" "$keys" "$operations" "$batch_size" "$value_bytes" "$seed" <<'PY'
import json
import platform
import subprocess
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
keys, operations, batch_size, value_bytes, seed = map(int, sys.argv[3:8])

def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"

records = []
for path in sorted((output_dir / "runs").glob("*/verify.json")):
    engine, case_name = path.parent.name.split(".", 1)
    record = json.loads(path.read_text())
    status = int((path.parent / "child.status").read_text().strip())
    expected_prefix = 64 if case_name == "old-state" else 80
    if record.get("accepted") is not True:
        raise SystemExit(f"verification was not accepted: {path}")
    if record.get("expected_prefix") != expected_prefix:
        raise SystemExit(f"wrong expected prefix in {path}: {record}")
    if record.get("reopen_passes") != 2:
        raise SystemExit(f"verification did not complete two reopens: {path}")
    if status != 137:
        raise SystemExit(f"child status was not SIGKILL for {path}: {status}")
    records.append({
        "engine": engine,
        "case": case_name,
        "requested_prefix": expected_prefix,
        "child_exit_code": status,
        "verifier": path.relative_to(output_dir).as_posix(),
        "accepted": True,
        "reopen_passes": record["reopen_passes"],
        "digest_fnv1a64": record["digest_fnv1a64"],
        "final_keys": record["final_keys"],
    })

if len(records) != 6:
    raise SystemExit(f"expected six process-termination cases, found {len(records)}")

manifest = {
    "format": "seerdb-common-kv-process-crash-manifest-v1",
    "repo_head": command_output("git", "-C", str(repo_root), "rev-parse", "HEAD"),
    "host_os": platform.system(),
    "host_arch": platform.machine(),
    "kernel": command_output("uname", "-r"),
    "durability": "durable",
    "workload": "batch-put",
    "keys": keys,
    "operations": operations,
    "batch_size": batch_size,
    "value_bytes": value_bytes,
    "seed": seed,
    "termination_boundary": "child SIGKILL after observing a pre-batch progress marker and while held before that batch commit",
    "accepted_states": {
        "old-state": "all batches before boundary are present; boundary batch is absent",
        "complete-new-state": "all batches through the preceding boundary are present; next boundary batch is absent",
    },
    "unsupported_boundaries": [
        "SIGKILL during fsync/page write is not exercised",
        "block-layer power loss and torn-write recovery are not exercised",
    ],
    "reopen_passes_required": 2,
    "cases": records,
    "accepted": all(record["accepted"] and record["reopen_passes"] == 2 for record in records),
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps(manifest, indent=2))
PY

echo "common_kv_faults: manifest $output_dir/manifest.json" >&2
