#!/usr/bin/env bash
set -Eeuo pipefail

# Run the dm-log-writes replay qualification on a disposable Linux host.
# This script intentionally requires root and fails when any prerequisite is
# missing; it is not a best-effort CI test.

usage() {
    echo "usage: SEERDB_POWERLOSS_ROOT=/dedicated/path $0 [ext4|xfs]" >&2
}

if [[ ${EUID:-$(id -u)} -ne 0 || ${OSTYPE:-} != linux* ]]; then
    echo "linux_power_loss: requires a privileged Linux host" >&2
    exit 2
fi

filesystem=${1:-ext4}
case "$filesystem" in
    ext4|xfs) ;;
    *) usage; exit 2 ;;
esac

: "${SEERDB_POWERLOSS_ROOT:?set SEERDB_POWERLOSS_ROOT to a dedicated writable filesystem}"
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
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
data_image=$work/data.img
baseline_image=$work/baseline.img
replay_image=$work/replay.img
log_image=$work/log.img
mount_point=$work/mnt
replay_mount=$work/replay-mnt
oracle=$work/oracle.txt
dm_name=seerdb-power-loss-$$
data_loop=
log_loop=
replay_loop=
dm_active=0
mounted=0

cleanup() {
    set +e
    if (( mounted )); then umount "$mount_point"; fi
    if (( dm_active )); then dmsetup remove "$dm_name"; fi
    [[ -n "$data_loop" ]] && losetup -d "$data_loop"
    [[ -n "$log_loop" ]] && losetup -d "$log_loop"
    [[ -n "$replay_loop" ]] && losetup -d "$replay_loop"
    rm -rf "$work"
}
trap cleanup EXIT

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
"$verify_binary" seed "$mount_point/db" "$oracle"
umount "$mount_point"
mounted=0
losetup -d "$data_loop"
data_loop=

cp --reflink=auto --sparse=always "$data_image" "$baseline_image"
cp --reflink=auto --sparse=always "$baseline_image" "$replay_image"

data_loop=$(losetup --find --show "$data_image")
log_loop=$(losetup --find --show "$log_image")
sectors=$(blockdev --getsz "$data_loop")
dmsetup create "$dm_name" --table "0 $sectors log-writes $data_loop $log_loop"
dm_active=1
mount "/dev/mapper/$dm_name" "$mount_point"
mounted=1
dmsetup message "$dm_name" 0 mark baseline
"$verify_binary" mutate "$mount_point/db" "$oracle"
dmsetup message "$dm_name" 0 mark workload-end
umount "$mount_point"
mounted=0
dmsetup remove "$dm_name"
dm_active=0
losetup -d "$data_loop"
data_loop=

replay_loop=$(losetup --find --show "$replay_image")
check_script=$repo_root/tools/seerdb_power_loss_check.sh
replay-log \
    --log "$log_loop" \
    --replay "$replay_loop" \
    --start-mark baseline \
    --end-mark workload-end \
    --fsck "$check_script '$replay_loop' '$replay_mount' '$oracle' '$verify_binary'" \
    --check flush

echo "linux_power_loss: PASS filesystem=$filesystem replayed durable flush prefixes"
