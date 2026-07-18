#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 4 ]]; then
    echo "usage: $0 <replay-loop-device> <mount-point> <oracle-file> <verify-binary>" >&2
    exit 2
fi

replay_device=$1
mount_point=$2
oracle_file=$3
verify_binary=$4

mounted=0
cleanup() {
    if (( mounted )); then
        umount "$mount_point"
    fi
}
trap cleanup EXIT

mount "$replay_device" "$mount_point"
mounted=1
"$verify_binary" verify "$mount_point/db" "$oracle_file"
