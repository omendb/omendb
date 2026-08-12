#!/usr/bin/env bash
set -Eeuo pipefail

# Run the common ordered-KV harness as a repeatable Linux qualification.
# The harness remains the source of truth for each v4 result; this wrapper
# fixes the matrix, preserves raw runs, and writes a v1 aggregate.

usage() {
    cat >&2 <<'EOF'
usage: tools/common_kv_qualification.sh [options]

Options:
  --output-dir PATH       Fresh directory for raw runs and manifest
                          (default: /tmp/seerdb-common-kv-qualification-TIMESTAMP)
  --engines LIST          Comma-separated: seerdb,fjall,rocksdb
                          (default: seerdb,fjall,rocksdb)
  --workloads LIST        Comma-separated workload names
                          (default: batch-put,mixed)
  --repetitions N         Measured repetitions per engine/workload (default: 3)
  --warmups N             Discarded warm-ups per engine/workload (default: 1)
  --keys N                Key-space size (default: 1000)
  --operations N          Measured operations (default: 4000)
  --batch-size N          One write batch size (default: 16)
  --batch-sizes LIST      Comma-separated write batch sizes; overrides
                          --batch-size (for example: 1,4,16)
  --value-bytes N         Value size (default: 128)
  --range-width N         Range width (default: 32)
  --seed N                Deterministic trace seed (default: 7)

Set SEERDB_BENCH_CPUSET (for example, 2) to run each benchmark under
taskset. The wrapper does not drop the filesystem cache; the manifest records
that policy. Set SEERDB_COMMON_KV_ALLOW_NONLINUX=1 only for local diagnostics.
EOF
}

die() {
    echo "common_kv_qualification: $*" >&2
    exit 2
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${SEERDB_COMMON_KV_OUTPUT:-"/tmp/seerdb-common-kv-qualification-$(date -u +%Y%m%dT%H%M%SZ)"}
engines_csv=seerdb,fjall,rocksdb
workloads_csv=batch-put,mixed
repetitions=3
warmups=1
keys=1000
operations=4000
batch_sizes_csv=16
value_bytes=128
range_width=32
seed=7

while (($#)); do
    case "$1" in
        --output-dir) output_dir=${2:?missing value for --output-dir}; shift 2 ;;
        --engines) engines_csv=${2:?missing value for --engines}; shift 2 ;;
        --workloads) workloads_csv=${2:?missing value for --workloads}; shift 2 ;;
        --repetitions) repetitions=${2:?missing value for --repetitions}; shift 2 ;;
        --warmups) warmups=${2:?missing value for --warmups}; shift 2 ;;
        --keys) keys=${2:?missing value for --keys}; shift 2 ;;
        --operations) operations=${2:?missing value for --operations}; shift 2 ;;
        --batch-size) batch_sizes_csv=${2:?missing value for --batch-size}; shift 2 ;;
        --batch-sizes) batch_sizes_csv=${2:?missing value for --batch-sizes}; shift 2 ;;
        --value-bytes) value_bytes=${2:?missing value for --value-bytes}; shift 2 ;;
        --range-width) range_width=${2:?missing value for --range-width}; shift 2 ;;
        --seed) seed=${2:?missing value for --seed}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) usage; die "unknown argument: $1" ;;
    esac
done

if [[ $(uname -s) != Linux && ${SEERDB_COMMON_KV_ALLOW_NONLINUX:-0} != 1 ]]; then
    die "requires Linux for the qualification matrix; set SEERDB_COMMON_KV_ALLOW_NONLINUX=1 for diagnostics"
fi

for command in cargo git python3 uname; do
    command -v "$command" >/dev/null || die "missing required command: $command"
done

is_positive_integer() {
    [[ $1 =~ ^[1-9][0-9]*$ ]]
}

is_nonnegative_integer() {
    [[ $1 =~ ^[0-9]+$ ]]
}

is_positive_integer "$repetitions" || die "--repetitions must be a positive integer"
is_nonnegative_integer "$warmups" || die "--warmups must be a non-negative integer"
for value in "$keys" "$operations" "$value_bytes" "$range_width"; do
    is_positive_integer "$value" || die "workload sizes must be positive integers"
done
[[ $seed =~ ^[0-9]+$ ]] || die "--seed must be a non-negative integer"

IFS=',' read -r -a engines <<< "$engines_csv"
IFS=',' read -r -a workloads <<< "$workloads_csv"
IFS=',' read -r -a batch_sizes <<< "$batch_sizes_csv"
((${#engines[@]} > 0)) || die "--engines cannot be empty"
((${#workloads[@]} > 0)) || die "--workloads cannot be empty"
((${#batch_sizes[@]} > 0)) || die "--batch-sizes cannot be empty"

seen_batch_sizes=""
for batch_size in "${batch_sizes[@]}"; do
    is_positive_integer "$batch_size" || die "--batch-sizes must contain positive integers"
    case ",$seen_batch_sizes," in
        *",$batch_size,"*) die "--batch-sizes must not contain duplicates" ;;
    esac
    seen_batch_sizes="${seen_batch_sizes:+$seen_batch_sizes,}$batch_size"
done

for engine in "${engines[@]}"; do
    case "$engine" in
        seerdb|fjall|rocksdb) ;;
        *) die "unknown engine '$engine'" ;;
    esac
done
for workload in "${workloads[@]}"; do
    case "$workload" in
        batch-put|mixed|point-read|range-read) ;;
        *) die "unknown workload '$workload'" ;;
    esac
done

if [[ -n ${SEERDB_BENCH_CPUSET:-} ]]; then
    command -v taskset >/dev/null || die "SEERDB_BENCH_CPUSET is set but taskset is unavailable"
fi

[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
mkdir -p "$output_dir/runs" "$output_dir/db" "$output_dir/traces"

build_root=$(mktemp -d "${TMPDIR:-/tmp}/seerdb-common-kv-build.XXXXXX")
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
                echo "common_kv_qualification: failed to build $engine" >&2
                return 1
            fi
            ;;
        rocksdb)
            if ! cargo build --release --manifest-path "$bench_manifest" --target-dir "$target_dir" \
                --no-default-features --features rocksdb >&2; then
                echo "common_kv_qualification: failed to build $engine" >&2
                return 1
            fi
            ;;
        *)
            echo "common_kv_qualification: unknown engine $engine" >&2
            return 1
    esac
    if [[ ! -x "$binary" ]]; then
        echo "common_kv_qualification: build succeeded but binary is missing: $binary" >&2
        return 1
    fi
    echo "$binary"
}

for engine in "${engines[@]}"; do
    echo "common_kv_qualification: building $engine" >&2
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
    esac
}

run_with_prefix() {
    if [[ -n ${SEERDB_BENCH_CPUSET:-} ]]; then
        taskset -c "$SEERDB_BENCH_CPUSET" "$@"
    else
        "$@"
    fi
}

run_benchmark() {
    local binary=$1
    local engine=$2
    local workload=$3
    local batch_size=$4
    local label=$5
    local db_path="$output_dir/db/$label"
    local output_path="$output_dir/runs/$label.json"
    local stdout_path="$output_dir/runs/$label.stdout"
    local trace_path="$output_dir/traces/${workload}-b${batch_size}.json"

    echo "common_kv_qualification: run $label" >&2
    run_with_prefix "$binary" \
        --engine "$engine" \
        --workload "$workload" \
        --durability durable \
        --path "$db_path" \
        --output "$output_path" \
        --trace-output "$trace_path" \
        --keys "$keys" \
        --operations "$operations" \
        --batch-size "$batch_size" \
        --value-bytes "$value_bytes" \
        --range-width "$range_width" \
        --seed "$seed" >"$stdout_path"
}

for engine in "${engines[@]}"; do
    for workload in "${workloads[@]}"; do
        for batch_size in "${batch_sizes[@]}"; do
            if ((warmups > 0)); then
                for ((warmup = 1; warmup <= warmups; warmup++)); do
                    warmup_label="warmup-${engine}-${workload}-b${batch_size}-${warmup}"
                    warmup_path="$output_dir/db/$warmup_label"
                    warmup_log="$output_dir/runs/$warmup_label.stdout"
                    echo "common_kv_qualification: warm-up $warmup_label" >&2
                    run_with_prefix "$(binary_for_engine "$engine")" \
                        --engine "$engine" --workload "$workload" --durability durable \
                        --path "$warmup_path" --keys "$keys" --operations "$operations" \
                        --batch-size "$batch_size" --value-bytes "$value_bytes" \
                        --range-width "$range_width" --seed "$seed" >"$warmup_log"
                    rm -rf -- "$warmup_path"
                done
            fi
            for ((repetition = 1; repetition <= repetitions; repetition++)); do
                label="${engine}-${workload}-b${batch_size}-r${repetition}"
                run_benchmark "$(binary_for_engine "$engine")" "$engine" "$workload" "$batch_size" "$label"
            done
        done
    done
done

python3 - "$output_dir" "$repo_root" "$engines_csv" "$workloads_csv" \
    "$repetitions" "$warmups" "$keys" "$operations" "$batch_sizes_csv" \
    "$value_bytes" "$range_width" "$seed" <<'PY'
import json
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
engines = sys.argv[3].split(",")
workloads = sys.argv[4].split(",")
repetitions = int(sys.argv[5])
warmups = int(sys.argv[6])
keys, operations = map(int, sys.argv[7:9])
batch_sizes = [int(value) for value in sys.argv[9].split(",")]
value_bytes, range_width, seed = map(int, sys.argv[10:13])

def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"

run_files = sorted((output_dir / "runs").glob("*.json"))
runs = [json.loads(path.read_text()) for path in run_files]
expected = len(engines) * len(workloads) * len(batch_sizes) * repetitions
if len(runs) != expected:
    raise SystemExit(f"expected {expected} measured JSON results, found {len(runs)}")

trace_artifacts = {}
for workload in workloads:
    for batch_size in batch_sizes:
        trace_key = f"{workload}/batch-{batch_size}"
        path = output_dir / "traces" / f"{workload}-b{batch_size}.json"
        if not path.is_file():
            raise SystemExit(f"missing exact trace artifact for {trace_key}: {path}")
        trace = json.loads(path.read_text())
        if trace.get("format") != "seerdb-common-kv-trace-v1":
            raise SystemExit(f"unexpected trace format for {trace_key}: {trace.get('format')}")
        if (
            trace.get("workload") != workload
            or trace.get("batch_size") != batch_size
            or trace.get("trace_operation_count") != operations
        ):
            raise SystemExit(f"trace parameters do not match qualification for {trace_key}")
        trace_artifacts[trace_key] = {
            "path": path.relative_to(output_dir).as_posix(),
            "trace_digest_fnv1a64": trace["trace_digest_fnv1a64"],
            "trace_operation_count": trace["trace_operation_count"],
        }

def median_metric(records, name):
    return statistics.median(record[name] for record in records)

summary = {}
for engine in engines:
    for workload in workloads:
        for batch_size in batch_sizes:
            records = [
                record for record in runs
                if record["engine"] == engine
                and record["workload"] == workload
                and record["batch_size"] == batch_size
            ]
            summary_key = f"{engine}/{workload}/batch-{batch_size}"
            if len(records) != repetitions:
                raise SystemExit(
                    f"expected {repetitions} records for {summary_key}, found {len(records)}"
                )
            digests = {record["digest_fnv1a64"] for record in records}
            if len(digests) != 1:
                raise SystemExit(f"digest mismatch across repetitions for {summary_key}: {sorted(digests)}")
            trace_key = f"{workload}/batch-{batch_size}"
            trace_digests = {record["trace_digest_fnv1a64"] for record in records}
            expected_trace_digest = trace_artifacts[trace_key]["trace_digest_fnv1a64"]
            if trace_digests != {expected_trace_digest}:
                raise SystemExit(
                    f"trace mismatch for {summary_key}: "
                    f"runs={sorted(trace_digests)} artifact={expected_trace_digest}"
                )
            summary[summary_key] = {
                "repetitions": len(records),
                "batch_size": batch_size,
                "digest_fnv1a64": records[0]["digest_fnv1a64"],
                "trace_digest_fnv1a64": expected_trace_digest,
                "throughput_ops_per_sec_median": median_metric(records, "throughput_ops_per_sec"),
                "throughput_ops_per_sec_min": min(record["throughput_ops_per_sec"] for record in records),
                "throughput_ops_per_sec_max": max(record["throughput_ops_per_sec"] for record in records),
                "p50_ns_median": median_metric(records, "p50_ns"),
                "p95_ns_median": median_metric(records, "p95_ns"),
                "p99_ns_median": median_metric(records, "p99_ns"),
                "reopen_ns_median": median_metric(records, "reopen_ns"),
                "disk_bytes_median": median_metric(records, "disk_bytes"),
                "process_max_rss_bytes_max": max(record["process_max_rss_bytes"] for record in records),
            }

manifest = {
    "format": "seerdb-common-kv-qualification-v2",
    "repo_head": command_output("git", "-C", str(repo_root), "rev-parse", "HEAD"),
    "durability": "durable",
    "cache_policy": "filesystem cache left intact; no cache drop performed",
    "cpu_affinity": os.environ.get("SEERDB_BENCH_CPUSET", "unbound"),
    "parameters": {
        "engines": engines,
        "workloads": workloads,
        "repetitions": repetitions,
        "warmups": warmups,
        "keys": keys,
        "operations": operations,
        "batch_sizes": batch_sizes,
        "value_bytes": value_bytes,
        "range_width": range_width,
        "seed": seed,
    },
    "trace_artifacts": trace_artifacts,
    "environment": {
        "os": platform.platform(),
        "kernel": command_output("uname", "-srmo"),
        "arch": platform.machine(),
        "rustc": command_output("rustc", "--version"),
        "cargo": command_output("cargo", "--version"),
        "cpu_count": os.cpu_count(),
    },
    "summary": summary,
    "runs": runs,
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(f"common_kv_qualification: manifest={output_dir / 'manifest.json'}")
PY
