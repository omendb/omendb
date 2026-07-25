#!/usr/bin/env bash
set -Eeuo pipefail

# Run the dm-log-writes replay qualification on a disposable Linux host.
# This script intentionally requires root and fails when any prerequisite is
# missing; it is not a best-effort CI test.

usage() {
    echo "usage: SEERDB_POWERLOSS_ROOT=/dedicated/path SEERDB_POWERLOSS_ARTIFACTS=/output/path $0 [ext4|xfs] [whole|segmented]" >&2
}

filesystem=${1:-ext4}
case "$filesystem" in
    ext4|xfs) ;;
    *) usage; exit 2 ;;
esac

layout=${2:-whole}
case "$layout" in
    whole|segmented) ;;
    *) usage; exit 2 ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
artifact_dir=${SEERDB_POWERLOSS_ARTIFACTS:-}
work=
work_started=0
data_image=
baseline_image=
prefix_image=
log_image=
mount_point=
replay_mount=
oracle=
dm_name=seerdb-power-loss-$$
data_loop=
log_loop=
prefix_loop=
dm_active=0
mounted=0
run_status=not-run
prefixes=0
current_prefix=0
preserved_run_dir=

write_manifest() {
    local exit_code=$1
    local manifest_path="$artifact_dir/seerdb-power-loss-manifest.env"
    local git_head
    local kernel
    local dmsetup_version
    git_head=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo unknown)
    kernel=$(uname -srmo 2>/dev/null || echo unavailable)
    dmsetup_version=$(dmsetup version 2>/dev/null | tr '\n' ' ' || echo unavailable)
    mkdir -p "$artifact_dir"
    {
        printf 'manifest_version=seerdb-power-loss-manifest-v1\n'
        printf 'status=%s\n' "$run_status"
        printf 'exit_code=%s\n' "$exit_code"
        printf 'filesystem=%s\n' "$filesystem"
        printf 'blob_layout=%s\n' "$layout"
        printf 'completed_prefixes=%s\n' "$prefixes"
        printf 'failed_prefix=%s\n' "$current_prefix"
        printf 'image_size=%s\n' "${SEERDB_POWERLOSS_IMAGE_SIZE:-512M}"
        printf 'log_size=%s\n' "${SEERDB_POWERLOSS_LOG_SIZE:-1G}"
        printf 'repo_head=%s\n' "$git_head"
        printf 'kernel=%s\n' "$kernel"
        printf 'dmsetup_version=%s\n' "$dmsetup_version"
        printf 'preserved_run_dir=%s\n' "$preserved_run_dir"
        printf 'oracle=%s\n' "$artifact_dir/oracle.txt"
    } > "$manifest_path"
    echo "linux_power_loss: manifest=$manifest_path" >&2
}

cleanup() {
    local exit_code=$?
    set +e
    if (( mounted )); then umount "$mount_point"; fi
    if (( dm_active )); then dmsetup remove "$dm_name"; fi
    [[ -n "$data_loop" ]] && losetup -d "$data_loop" || true
    [[ -n "$log_loop" ]] && losetup -d "$log_loop" || true
    [[ -n "$prefix_loop" ]] && losetup -d "$prefix_loop" || true
    if (( exit_code == 0 )); then
        run_status=pass
    elif (( work_started )); then
        run_status=fail
    else
        run_status=not-run
    fi
    if [[ -n "$artifact_dir" ]]; then
        mkdir -p "$artifact_dir"
        if (( exit_code != 0 )) && (( work_started )) && [[ -d "$work" ]]; then
            preserved_run_dir="$artifact_dir/replay-${filesystem}-${layout}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
            mkdir -p "$preserved_run_dir"
            cp -a "$work/." "$preserved_run_dir/"
        fi
        if [[ -n "$oracle" && -f "$oracle" ]]; then
            cp "$oracle" "$artifact_dir/oracle.txt"
        fi
        write_manifest "$exit_code"
    fi
    if (( work_started )) && [[ -d "$work" ]]; then
        rm -rf "$work"
    fi
    exit "$exit_code"
}
trap cleanup EXIT

if [[ ${EUID:-$(id -u)} -ne 0 || ${OSTYPE:-} != linux* ]]; then
    echo "linux_power_loss: requires a privileged Linux host" >&2
    exit 2
fi

: "${SEERDB_POWERLOSS_ROOT:?set SEERDB_POWERLOSS_ROOT to a dedicated writable filesystem}"
for command in blockdev cargo cp dmsetup losetup mount mkfs."$filesystem" replay-log truncate umount; do
    command -v "$command" >/dev/null || {
        echo "linux_power_loss: missing required command: $command" >&2
        exit 2
    }
done

if ! dmsetup targets | awk '{print $1}' | grep -qx 'log-writes'; then
    echo "linux_power_loss: dm-log-writes target is unavailable" >&2
    exit 2
fi

work=$(mktemp -d "$SEERDB_POWERLOSS_ROOT/seerdb-power-loss.XXXXXX")
work_started=1
data_image=$work/data.img
baseline_image=$work/baseline.img
prefix_image=$work/prefix.img
log_image=$work/log.img
mount_point=$work/mnt
replay_mount=$work/replay-mnt
oracle=$work/oracle.txt

mkdir -p "$mount_point" "$replay_mount"
truncate -s "${SEERDB_POWERLOSS_IMAGE_SIZE:-512M}" "$data_image"
truncate -s "${SEERDB_POWERLOSS_LOG_SIZE:-1G}" "$log_image"

verify_binary=$repo_root/target/release/examples/seerdb_power_loss
cargo build --release --example seerdb_power_loss --manifest-path "$repo_root/Cargo.toml"

data_loop=$(losetup --find --show "$data_image")
mkfs."$filesystem" -F "$data_loop" >/dev/null
mount "$data_loop" "$mount_point"
mounted=1
mkdir "$mount_point/db"
SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
    "$verify_binary" seed "$mount_point/db" "$oracle"
umount "$mount_point"
mounted=0
losetup -d "$data_loop"
data_loop=

cp --reflink=auto --sparse=always "$data_image" "$baseline_image"

data_loop=$(losetup --find --show "$data_image")
log_loop=$(losetup --find --show "$log_image")
sectors=$(blockdev --getsz "$data_loop")
dmsetup create "$dm_name" --table "0 $sectors log-writes $data_loop $log_loop"
dm_active=1
mount "/dev/mapper/$dm_name" "$mount_point"
mounted=1
dmsetup message "$dm_name" 0 mark baseline
SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
    "$verify_binary" mutate "$mount_point/db" "$oracle"
dmsetup message "$dm_name" 0 mark workload-end
umount "$mount_point"
mounted=0
dmsetup remove "$dm_name"
dm_active=0
losetup -d "$data_loop"
data_loop=

# The log is now immutable input. Keep it detached from the log-writes target
# before replay so qualification cannot accidentally alter its source.
losetup -d "$log_loop"
log_loop=$(losetup --read-only --find --show "$log_image")

check_script=$repo_root/tools/seerdb_power_loss_check.sh

# replay-log's --fsck/--check mode invokes the checker while it continues to
# replay the same device. The checker opens SeerDB normally, so recovery and
# cleanup can change that device before later prefixes are tested. Discover
# each flush boundary first, then replay every prefix into a fresh copy of the
# baseline image. The source baseline and log are never mounted or modified by
# verification.
baseline_entry=$(replay-log \
    --log "$log_loop" \
    --find \
    --end-mark baseline)
end_entry=$(replay-log \
    --log "$log_loop" \
    --find \
    --start-mark baseline \
    --end-mark workload-end)
if (( end_entry <= baseline_entry )); then
    echo "linux_power_loss: invalid replay marks baseline=$baseline_entry workload-end=$end_entry" >&2
    exit 1
fi

prefixes=0
cursor=$baseline_entry
while :; do
    current_prefix=$((prefixes + 1))
    final_boundary=0
    if next_entry=$(replay-log \
        --log "$log_loop" \
        --find \
        --start-entry "$cursor" \
        --next-flush); then
        if (( next_entry > end_entry )); then
            echo "linux_power_loss: replay boundary $next_entry passed workload-end $end_entry" >&2
            exit 1
        fi
    else
        # The workload-end mark follows the final mutate flush, but it is
        # still checked explicitly if no later flush exists.
        next_entry=$end_entry
        final_boundary=1
    fi

    rm -f "$prefix_image"
    cp --reflink=auto --sparse=always "$baseline_image" "$prefix_image"
    prefix_loop=$(losetup --find --show "$prefix_image")
    count=$((next_entry - baseline_entry))
    if (( count <= 0 )); then
        echo "linux_power_loss: invalid non-forward replay boundary $next_entry" >&2
        exit 1
    fi
    replay-log \
        --log "$log_loop" \
        --replay "$prefix_loop" \
        --start-entry "$baseline_entry" \
        --limit "$count"
    SEERDB_POWERLOSS_BLOB_STORAGE="$layout" \
        "$check_script" "$prefix_loop" "$replay_mount" "$oracle" "$verify_binary"
    losetup -d "$prefix_loop"
    prefix_loop=
    prefixes=$current_prefix

    if (( next_entry == end_entry || final_boundary )); then
        break
    fi
    cursor=$next_entry
done

echo "linux_power_loss: PASS filesystem=$filesystem layout=$layout independent_prefixes=$prefixes"
