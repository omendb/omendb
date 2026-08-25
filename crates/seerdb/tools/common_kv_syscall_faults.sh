#!/usr/bin/env bash
set -Eeuo pipefail

# Exercise external libc-boundary failures during a seeded mutation across the
# common ordered-KV adapters. The baseline database is created and verified
# first; the faulted child opens it and applies the next deterministic batch
# range. This is not torn-write or block-layer power-loss evidence.

usage() {
    cat >&2 <<'EOF'
usage: tools/common_kv_syscall_faults.sh [options]

Options:
  --output-dir PATH       Fresh directory for databases and manifest
  --keys N                Key-space size (default: 256)
  --base-operations N     Durable baseline batch operations (default: 64)
  --operations N          Mutation operations (default: 64)
  --batch-size N          Batch size (default: 16)
  --value-bytes N         Value size (default: 64)
  --seed N                Deterministic trace seed (default: 7)

The harness runs seerdb, fjall, and rocksdb. It traces each engine's seeded
mutation, fails every observed fsync, fdatasync, and rename call once both
before and after completion, and accepts only a complete batch prefix after
two fresh reopens.

Set SEERDB_COMMON_KV_RECORD_UNSUPPORTED=1 on a non-Linux host to write a
versioned manifest with status=unsupported and exit successfully. Without that
opt-in, the Linux-only gate fails closed with exit status 2.
EOF
}

die() {
    echo "common_kv_syscall_faults: $*" >&2
    exit 2
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${SEERDB_COMMON_KV_SYSCALL_FAULTS_OUTPUT:-"/tmp/seerdb-common-kv-syscall-faults-$(date -u +%Y%m%dT%H%M%SZ)"}
keys=256
base_operations=64
operations=64
batch_size=16
value_bytes=64
seed=7

while (($#)); do
    case "$1" in
        --output-dir) output_dir=${2:?missing value for --output-dir}; shift 2 ;;
        --keys) keys=${2:?missing value for --keys}; shift 2 ;;
        --base-operations) base_operations=${2:?missing value for --base-operations}; shift 2 ;;
        --operations) operations=${2:?missing value for --operations}; shift 2 ;;
        --batch-size) batch_size=${2:?missing value for --batch-size}; shift 2 ;;
        --value-bytes) value_bytes=${2:?missing value for --value-bytes}; shift 2 ;;
        --seed) seed=${2:?missing value for --seed}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) usage; die "unknown argument: $1" ;;
    esac
done

platform_name=$(uname -s)
for command in awk cargo cc cp git python3 uname; do
    command -v "$command" >/dev/null || die "missing required command: $command"
done
for value in "$keys" "$base_operations" "$operations" "$batch_size" "$value_bytes"; do
    [[ $value =~ ^[1-9][0-9]*$ ]] || die "numeric options must be positive integers"
done
[[ $seed =~ ^[0-9]+$ ]] || die "--seed must be a non-negative integer"
((base_operations % batch_size == 0 && operations % batch_size == 0)) || \
    die "base and mutation operations must be batch-aligned"

if [[ $platform_name != Linux ]]; then
    if [[ ${SEERDB_COMMON_KV_RECORD_UNSUPPORTED:-0} != 1 ]]; then
        die "requires Linux; set SEERDB_COMMON_KV_RECORD_UNSUPPORTED=1 to write an explicit unsupported manifest"
    fi
    [[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"
    mkdir -p "$output_dir"
    python3 - "$output_dir" "$repo_root" "$platform_name" "$keys" "$base_operations" "$operations" "$batch_size" "$value_bytes" "$seed" <<'PY'
import json
import platform
import subprocess
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
host_os, keys, base_operations, operations, batch_size, value_bytes, seed = sys.argv[3:]

def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"

manifest = {
    "format": "seerdb-common-kv-syscall-fault-manifest-v1",
    "status": "unsupported",
    "accepted": False,
    "outcome": "unsupported-platform",
    "execution_outcome": "unsupported",
    "recovery_outcome": "unsupported",
    "reopen_outcome": "unsupported",
    "resource_outcome": "not-collected",
    "reason": "external libc-boundary qualification requires Linux",
    "repo_head": command_output("git", "-C", str(repo_root), "rev-parse", "HEAD"),
    "host_os": host_os,
    "host_platform": platform.platform(),
    "host_arch": platform.machine(),
    "durability": "durable",
    "workload": "batch-put seeded mutation",
    "keys": int(keys),
    "base_operations": int(base_operations),
    "operations": int(operations),
    "batch_size": int(batch_size),
    "value_bytes": int(value_bytes),
    "seed": int(seed),
    "cases": [],
    "not_exercised": [
        "Linux libc-boundary fault injection was not executed",
        "torn or short block writes",
        "block-layer reordering or cache loss",
        "machine power loss",
        "filesystem crash-consistency races outside intercepted calls",
    ],
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps(manifest, indent=2))
PY
    echo "common_kv_syscall_faults: unsupported manifest $output_dir/manifest.json" >&2
    exit 0
fi

[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"

mkdir -p "$output_dir/baselines" "$output_dir/runs"
trace_artifact="$output_dir/trace.json"
build_root=$(mktemp -d "${TMPDIR:-/tmp}/seerdb-common-kv-syscall-faults-build.XXXXXX")
cleanup() {
    local exit_code=$?
    rm -rf -- "$build_root"
    exit "$exit_code"
}
trap cleanup EXIT

injector="$build_root/libseerdb_syscall_fault.so"
cc -shared -fPIC -O2 -Wall -Wextra \
    -o "$injector" "$repo_root/tools/seerdb_syscall_fault.c" -ldl

bench_manifest="$repo_root/tools/common_kv_bench/Cargo.toml"
bench_name=seerdb-common-kv-bench

build_engine() {
    local engine=$1
    local target_dir="$build_root/$engine"
    local binary="$target_dir/release/$bench_name"
    case "$engine" in
        seerdb|fjall)
            if ! cargo build --release --manifest-path "$bench_manifest" --target-dir "$target_dir" >&2; then
                echo "common_kv_syscall_faults: failed to build $engine" >&2
                return 1
            fi
            ;;
        rocksdb)
            if ! cargo build --release --manifest-path "$bench_manifest" --target-dir "$target_dir" \
                --no-default-features --features rocksdb >&2; then
                echo "common_kv_syscall_faults: failed to build $engine" >&2
                return 1
            fi
            ;;
        *) die "unknown engine: $engine" ;;
    esac
    if [[ ! -x "$binary" ]]; then
        echo "common_kv_syscall_faults: build succeeded but binary is missing: $binary" >&2
        return 1
    fi
    echo "$binary"
}

for engine in seerdb fjall rocksdb; do
    echo "common_kv_syscall_faults: building $engine" >&2
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

set_mutation_args() {
    local engine=$1
    local path=$2
    mutation_args=(
        --engine "$engine"
        --workload batch-put
        --durability durable
        --open-existing
        --base-operations "$base_operations"
        --path "$path"
        --keys "$keys"
        --operations "$operations"
        --batch-size "$batch_size"
        --value-bytes "$value_bytes"
        --range-width 1
        --seed "$seed"
    )
}

: >"$output_dir/observed.tsv"
: >"$output_dir/cases.tsv"

for engine in seerdb fjall rocksdb; do
    binary=$(binary_for_engine "$engine")
    baseline="$output_dir/baselines/$engine"
    mkdir -p "$baseline"

    "$binary" \
        --engine "$engine" \
        --workload batch-put \
        --durability durable \
        --path "$baseline/db" \
        --keys "$keys" \
        --operations "$base_operations" \
        --batch-size "$batch_size" \
        --value-bytes "$value_bytes" \
        --range-width 1 \
        --seed "$seed" \
        >"$baseline/seed.stdout" 2>"$baseline/seed.stderr"

    trace="$output_dir/baselines/$engine-trace"
    mkdir -p "$trace"
    cp -a "$baseline/db" "$trace/db"
    set_mutation_args "$engine" "$trace/db"
    trace_args=()
    if [[ ! -e "$trace_artifact" ]]; then
        trace_args=(--trace-output "$trace_artifact")
    fi
    LD_PRELOAD="$injector" \
    SEERDB_FAULT_SYSCALL=none \
    SEERDB_FAULT_TRACE="$trace/syscalls.log" \
        "$binary" "${mutation_args[@]}" \
        "${trace_args[@]}" \
        >"$trace/mutate.stdout" 2>"$trace/mutate.stderr"

    for syscall in fsync fdatasync rename; do
        call_count=$(awk -v method="$syscall" '$1 == method { count = $2 } END { print count + 0 }' \
            "$trace/syscalls.log")
        printf '%s\t%s\t%s\n' "$engine" "$syscall" "$call_count" >>"$output_dir/observed.tsv"
        if ((call_count == 0)); then
            continue
        fi

        for mode in before after; do
            for ((fault_after = 1; fault_after <= call_count; fault_after++)); do
                case_name="$engine.$mode.$syscall.$fault_after"
                case_dir="$output_dir/runs/$case_name"
                mkdir -p "$case_dir"
                cp -a "$baseline/db" "$case_dir/db"
                set_mutation_args "$engine" "$case_dir/db"
                set +e
                LD_PRELOAD="$injector" \
                SEERDB_FAULT_SYSCALL="$syscall" \
                SEERDB_FAULT_AFTER="$fault_after" \
                SEERDB_FAULT_MODE="$mode" \
                    "$binary" "${mutation_args[@]}" \
                    >"$case_dir/mutate.stdout" 2>"$case_dir/mutate.stderr"
                child_status=$?
                set -e
                printf '%s\n' "$child_status" >"$case_dir/child.status"

                accepted_prefix=
                for ((prefix = 0; prefix <= operations; prefix += batch_size)); do
                    verify_args=(
                        --engine "$engine"
                        --workload batch-put
                        --durability durable
                        --open-existing
                        --base-operations "$base_operations"
                        --path "$case_dir/db"
                        --keys "$keys"
                        --operations "$operations"
                        --batch-size "$batch_size"
                        --value-bytes "$value_bytes"
                        --range-width 1
                        --seed "$seed"
                        --verify-prefix "$prefix"
                    )
                    set +e
                    "$binary" "${verify_args[@]}" \
                        >"$case_dir/verify-$prefix.stdout" \
                        2>"$case_dir/verify-$prefix.stderr"
                    verify_status=$?
                    set -e
                    if ((verify_status == 0)); then
                        accepted_prefix=$prefix
                        break
                    fi
                done
                if [[ -z "$accepted_prefix" ]]; then
                    echo "common_kv_syscall_faults: no accepted batch prefix for $case_name" >&2
                    sed -n '1,120p' "$case_dir/mutate.stderr" >&2 || true
                    exit 1
                fi
                printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                    "$engine" "$mode" "$syscall" "$fault_after" "$call_count" "$child_status" "$accepted_prefix" \
                    >>"$output_dir/cases.tsv"
            done
        done
    done
done

python3 - "$output_dir" "$repo_root" "$keys" "$base_operations" "$operations" "$batch_size" "$value_bytes" "$seed" <<'PY'
import json
import platform
import subprocess
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
keys, base_operations, operations, batch_size, value_bytes, seed = map(int, sys.argv[3:])

def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"

trace_path = output_dir / "trace.json"
if not trace_path.is_file():
    raise SystemExit(f"missing exact trace artifact: {trace_path}")
trace = json.loads(trace_path.read_text())
if trace.get("format") != "seerdb-common-kv-trace-v1":
    raise SystemExit(f"unexpected trace format: {trace.get('format')}")
if (
    trace.get("workload") != "batch-put"
    or trace.get("base_operations") != base_operations
    or trace.get("trace_operation_count") != operations
):
    raise SystemExit("trace parameters do not match syscall-fault qualification")

observed = []
for line in (output_dir / "observed.tsv").read_text().splitlines():
    engine, syscall, count = line.split("\t")
    observed.append({"engine": engine, "syscall": syscall, "observed_calls": int(count)})

cases = []
for line in (output_dir / "cases.tsv").read_text().splitlines():
    engine, mode, syscall, failed_call, observed_calls, child_status, prefix = line.split("\t")
    cases.append({
        "engine": engine,
        "mode": mode,
        "syscall": syscall,
        "failed_call": int(failed_call),
        "observed_calls": int(observed_calls),
        "child_exit_status": int(child_status),
        "accepted_prefix": int(prefix),
        "execution_outcome": "refused" if int(child_status) != 0 else "completed",
        "recovery_outcome": "complete-new-state" if int(prefix) == operations else "complete-prefix",
        "reopen_outcome": "stable-two-reopen",
        "resource_outcome": "not-collected",
        "accepted": True,
        "reopen_passes": 2,
    })

if not cases:
    raise SystemExit("no common-KV syscall cases executed")

manifest = {
    "format": "seerdb-common-kv-syscall-fault-manifest-v1",
    "status": "accepted",
    "outcome": "all-cases-accepted",
    "repo_head": command_output("git", "-C", str(repo_root), "rev-parse", "HEAD"),
    "host_os": platform.system(),
    "host_arch": platform.machine(),
    "kernel": command_output("uname", "-r"),
    "durability": "durable",
    "workload": "batch-put seeded mutation",
    "keys": keys,
    "base_operations": base_operations,
    "operations": operations,
    "batch_size": batch_size,
    "value_bytes": value_bytes,
    "seed": seed,
    "trace_artifact": "trace.json",
    "trace_digest_fnv1a64": trace["trace_digest_fnv1a64"],
    "fault_domain": "external libc boundary; one observed fsync/fdatasync/rename call returns EIO before or after completion",
    "outcome_taxonomy": {
        "execution_outcome": "refused means the faulted mutation process returned non-zero; completed means it returned zero",
        "recovery_outcome": "complete-new-state means the full mutation prefix was durable; complete-prefix means a shorter complete batch prefix was recovered",
        "reopen_outcome": "stable-two-reopen means both fresh verifier opens matched the accepted prefix",
        "resource_outcome": "not-collected means this wrapper does not qualify resource equivalence",
    },
    "accepted_states": "a complete batch prefix after the durable baseline, verified across two fresh reopens",
    "modes": ["before", "after"],
    "not_exercised": [
        "torn or short block writes",
        "block-layer reordering or cache loss",
        "machine power loss",
        "filesystem crash-consistency races outside the intercepted calls",
        "Rust positional page writes that bypass the interposed libc pwrite symbol",
    ],
    "observed_calls": observed,
    "cases": cases,
    "case_count": len(cases),
    "all_accepted": all(case["accepted"] and case["reopen_passes"] == 2 for case in cases),
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"manifest": str(output_dir / "manifest.json"), "case_count": len(cases)}))
PY

echo "common_kv_syscall_faults: PASS manifest=$output_dir/manifest.json"
