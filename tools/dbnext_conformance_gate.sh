#!/usr/bin/env bash
set -Eeuo pipefail

# Run the local SeerDB-side DBNext R0 integrity/conformance gate and retain
# both component reports plus one aggregate manifest. The shared trace and
# fault schedule remain owned by the DBNext worktree.

usage() {
    echo "usage: $0 [TRACE] [FAILURE_SCHEDULE] [OUTPUT_DIR]" >&2
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
trace=${1:-"$repo_root/../monorepo-dbnext/experiments/dbnext/ai/workloads/r0-integrity-recovery/trace.jsonl"}
schedule=${2:-"$repo_root/../monorepo-dbnext/experiments/dbnext/ai/workloads/r0-integrity-recovery/failure-schedule.jsonl"}
output_dir=${3:-"${TMPDIR:-/tmp}/seerdb-dbnext-gate-$$"}

if [[ ! -f "$trace" || ! -f "$schedule" ]]; then
    usage
    echo "dbnext_conformance_gate: trace and failure schedule must exist" >&2
    exit 2
fi

mkdir -p "$output_dir"
replay_result="$output_dir/r0-replay.json"
replay_manifest="$output_dir/r0-replay-manifest.json"
fault_result="$output_dir/r0-faults.json"
fault_manifest="$output_dir/r0-fault-manifest.json"
gate_manifest="$output_dir/dbnext-conformance-gate-v1.json"

cargo run --release --example dbnext_r0_replay --manifest-path "$repo_root/Cargo.toml" -- \
    "$trace" --output "$replay_result" --manifest "$replay_manifest"
cargo run --release --all-features --example dbnext_r0_faults --manifest-path "$repo_root/Cargo.toml" -- \
    "$schedule" --output "$fault_result" --manifest "$fault_manifest"

python3 - "$trace" "$schedule" "$replay_result" "$fault_result" "$gate_manifest" <<'PY'
import json
import pathlib
import sys

trace, schedule, replay_path, fault_path, gate_path = map(pathlib.Path, sys.argv[1:])
replay = json.loads(replay_path.read_text())
faults = json.loads(fault_path.read_text())

replay_passed = (
    replay.get("adapter") == "seerdb-r0-v0"
    and replay.get("verification", {}).get("wal_bytes") == 0
    and bool(replay.get("state_digest"))
)
faults_passed = (
    faults.get("adapter") == "seerdb-r0-faults-v1"
    and faults.get("case_count") == 13
    and faults.get("accepted") is True
)

gate = {
    "manifest_version": "dbnext-conformance-gate-v1",
    "target": "seerdb-rust",
    "workload": "r0-integrity-recovery",
    "inputs": {
        "trace": str(trace.resolve()),
        "failure_schedule": str(schedule.resolve()),
    },
    "components": {
        "r0_replay": {
            "passed": replay_passed,
            "result": str(replay_path.resolve()),
            "manifest": str((replay_path.parent / "r0-replay-manifest.json").resolve()),
            "state_digest": replay.get("state_digest"),
            "commit_id": replay.get("commit_id"),
        },
        "r0_fault_outcomes": {
            "passed": faults_passed,
            "result": str(fault_path.resolve()),
            "manifest": str((fault_path.parent / "r0-fault-manifest.json").resolve()),
            "case_count": faults.get("case_count"),
            "accepted": faults.get("accepted"),
        },
    },
    "accepted": replay_passed and faults_passed,
    "unsupported": [
        "typed DBNext R1/R2 semantics are exercised by the sibling DBNext worktree",
        "Linux block-layer power-loss and external filesystem races require the privileged SeerDB runner",
    ],
}
gate_path.write_text(json.dumps(gate, indent=2) + "\n")
if not gate["accepted"]:
    raise SystemExit("DBNext integrity/conformance gate failed")
PY

echo "dbnext_conformance_gate: PASS output_dir=$output_dir manifest=$gate_manifest"
