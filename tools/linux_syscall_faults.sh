#!/usr/bin/env bash
set -Eeuo pipefail

# Run external libc-boundary fault injection against a real durable mutation.
# This is intentionally narrower than dm-log-writes: it tests returned EIO at
# fsync/fdatasync/rename, not torn writes, block reordering, or power loss.

usage() {
    cat >&2 <<'EOF'
usage: tools/linux_syscall_faults.sh [--output-dir PATH]

The default output is /tmp/seerdb-linux-syscall-faults-TIMESTAMP. Each
observed fsync, fdatasync, rename, and write call is failed once in fresh
whole-image and segmented databases. Every case must reopen as the old or
complete-new root and pass verification twice.
EOF
}

die() {
    echo "linux_syscall_faults: $*" >&2
    exit 2
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${SEERDB_SYSCALL_FAULTS_OUTPUT:-"/tmp/seerdb-linux-syscall-faults-$(date -u +%Y%m%dT%H%M%SZ)"}

while (($#)); do
    case "$1" in
        --output-dir) output_dir=${2:?missing value for --output-dir}; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) usage; die "unknown argument: $1" ;;
    esac
done

[[ $(uname -s) == Linux ]] || die "requires Linux"
for command in awk cargo cc cp git python3 uname; do
    command -v "$command" >/dev/null || die "missing required command: $command"
done
[[ ! -e "$output_dir" ]] || die "output directory already exists: $output_dir"

mkdir -p "$output_dir/baselines" "$output_dir/runs"
build_root=$(mktemp -d "${TMPDIR:-/tmp}/seerdb-linux-syscall-faults-build.XXXXXX")
cleanup() {
    local exit_code=$?
    rm -rf -- "$build_root"
    exit "$exit_code"
}
trap cleanup EXIT

injector="$build_root/libseerdb_syscall_fault.so"
cc -shared -fPIC -O2 -Wall -Wextra \
    -o "$injector" "$repo_root/tools/seerdb_syscall_fault.c" -ldl
cargo build --release --example seerdb_power_loss --manifest-path "$repo_root/Cargo.toml" >&2
verify_binary="$repo_root/target/release/examples/seerdb_power_loss"

: >"$output_dir/cases.tsv"
: >"$output_dir/observed.tsv"

for layout in whole segmented; do
    baseline="$output_dir/baselines/$layout"
    mkdir -p "$baseline"
    SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
        "$verify_binary" seed "$baseline/db" "$baseline/oracle" \
        >"$baseline/seed.stdout" 2>"$baseline/seed.stderr"

    trace_run="$output_dir/baselines/$layout-trace"
    mkdir -p "$trace_run"
    cp -a "$baseline/db" "$trace_run/db"
    cp "$baseline/oracle" "$trace_run/oracle"
    LD_PRELOAD="$injector" \
    SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
    SEERDB_FAULT_TRACE="$trace_run/syscalls.log" \
    SEERDB_FAULT_SYSCALL=none \
        "$verify_binary" mutate "$trace_run/db" "$trace_run/oracle" \
        >"$trace_run/mutate.stdout" 2>"$trace_run/mutate.stderr"

    for syscall in fsync fdatasync rename write; do
        call_count=$(awk -v method="$syscall" '$1 == method { count = $2 } END { print count + 0 }' \
            "$trace_run/syscalls.log")
        printf '%s\t%s\t%s\n' "$layout" "$syscall" "$call_count" >>"$output_dir/observed.tsv"
        if (( call_count == 0 )); then
            continue
        fi

        for ((fault_after = 1; fault_after <= call_count; fault_after++)); do
            case_name="$layout.$syscall.$fault_after"
            case_dir="$output_dir/runs/$case_name"
            mkdir -p "$case_dir"
            cp -a "$baseline/db" "$case_dir/db"
            cp "$baseline/oracle" "$case_dir/oracle"

            set +e
            LD_PRELOAD="$injector" \
            SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
            SEERDB_FAULT_SYSCALL="$syscall" \
            SEERDB_FAULT_AFTER="$fault_after" \
                "$verify_binary" mutate "$case_dir/db" "$case_dir/oracle" \
                >"$case_dir/mutate.stdout" 2>"$case_dir/mutate.stderr"
            child_status=$?
            set -e
            printf '%s\n' "$child_status" >"$case_dir/child.status"

            if ! SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
                "$verify_binary" verify-fault "$case_dir/db" "$baseline/oracle" \
                >"$case_dir/verify.stdout" 2>"$case_dir/verify.stderr"; then
                echo "linux_syscall_faults: verifier failed for $case_name" >&2
                sed -n '1,120p' "$case_dir/verify.stderr" >&2 || true
                exit 1
            fi
            printf '%s\t%s\t%s\t%s\t%s\n' \
                "$layout" "$syscall" "$fault_after" "$call_count" "$child_status" \
                >>"$output_dir/cases.tsv"
        done
    done
done

python3 - "$output_dir" "$repo_root" <<'PY'
import json
import platform
import subprocess
import sys
from pathlib import Path

output_dir = Path(sys.argv[1])
repo_root = Path(sys.argv[2])

def command_output(*command):
    try:
        return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"

observed = []
for line in (output_dir / "observed.tsv").read_text().splitlines():
    layout, syscall, count = line.split("\t")
    observed.append({"layout": layout, "syscall": syscall, "observed_calls": int(count)})

cases = []
for line in (output_dir / "cases.tsv").read_text().splitlines():
    layout, syscall, fault_after, observed_calls, child_status = line.split("\t")
    cases.append({
        "layout": layout,
        "syscall": syscall,
        "failed_call": int(fault_after),
        "observed_calls": int(observed_calls),
        "child_exit_status": int(child_status),
        "accepted": True,
        "reopen_passes": 2,
    })

if not cases:
    raise SystemExit("no external syscall cases executed")
for item in observed:
    if item["syscall"] in {"fsync", "rename", "write"} and item["observed_calls"] == 0:
        raise SystemExit(f"required syscall was not observed: {item}")

manifest = {
    "format": "seerdb-linux-syscall-fault-manifest-v1",
    "repo_head": command_output("git", "-C", str(repo_root), "rev-parse", "HEAD"),
    "host_os": platform.system(),
    "host_arch": platform.machine(),
    "kernel": command_output("uname", "-r"),
    "fault_domain": "libc boundary; one selected fsync/fdatasync/rename/write call returns EIO",
    "oracle": "baseline seed oracle remains outside the faulted child and is used by the verifier",
    "accepted_states": "old seeded root or complete-new mutation root",
    "verification": "fresh process opens each case twice, checks active and retained values, and runs DB verify",
    "not_exercised": [
        "Rust positional page writes that bypass the interposed libc pwrite symbol",
        "torn or short block writes",
        "block-layer reordering or cache loss",
        "machine power loss",
        "filesystem crash-consistency races outside the intercepted calls",
    ],
    "observed_calls": observed,
    "cases": cases,
    "case_count": len(cases),
    "all_accepted": all(item["accepted"] for item in cases),
}
(output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
print(json.dumps({"manifest": str(output_dir / "manifest.json"), "case_count": len(cases)}))
PY

echo "linux_syscall_faults: PASS manifest=$output_dir/manifest.json"
