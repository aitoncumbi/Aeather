#!/usr/bin/env bash
# rclone against Aether: copy, check (sizes and MD5s), sync, server-side
# copies and purge, with small cutoffs so that multipart paths are used.
set -euo pipefail
source "$(dirname "$0")/common.sh"

export RCLONE_CONFIG=/dev/null
export RCLONE_CONFIG_AETHER_TYPE=s3 RCLONE_CONFIG_AETHER_PROVIDER=Other
export RCLONE_CONFIG_AETHER_ENDPOINT=$COMPAT_ENDPOINT RCLONE_CONFIG_AETHER_REGION=us-east-1
export RCLONE_CONFIG_AETHER_ACCESS_KEY_ID=$COMPAT_ACCESS_KEY
export RCLONE_CONFIG_AETHER_SECRET_ACCESS_KEY=$COMPAT_SECRET_KEY
export RCLONE_CONFIG_AETHER_FORCE_PATH_STYLE=true
# `command`: without RCLONE, a bare `rclone` here would call this function again.
rclone() {
    command "${RCLONE:-rclone}" --s3-upload-cutoff 5M --s3-chunk-size 5M --s3-copy-cutoff 5M \
        --retries 1 --low-level-retries 1 "$@"
}
R=aether:compat-rclone

make_tree
rclone mkdir "$R"
rclone copy tree "$R/tree"
rclone check tree "$R/tree" --one-way 2> check.log && echo "ok: copy, then check sizes and hashes"
check "listing" 6 "$(rclone lsf -R --files-only "$R/tree" | wc -l)"

echo "changed" > tree/hello.txt
rm tree/empty
rclone sync tree "$R/tree"
rclone check tree "$R/tree" 2> check.log && echo "ok: sync after local changes"
check "sync deleted the removed file" 5 "$(rclone lsf -R --files-only "$R/tree" | wc -l)"

rclone copyto "$R/tree/large.bin" large.out
cmp tree/large.bin large.out && echo "ok: multipart download"

# Server-side: CopyObject for small files, UploadPartCopy above the cutoff.
rclone copy "$R/tree" "$R/copy"
rclone check tree "$R/copy" 2> check.log && echo "ok: server-side copies"
check "md5 of a multipart copy" "$(md5sum < tree/large.bin | cut -d' ' -f1)" \
    "$(rclone md5sum "$R/copy/large.bin" | cut -d' ' -f1)"

rclone moveto "$R/copy/hello.txt" "$R/moved.txt"
check "moved object" changed "$(rclone cat "$R/moved.txt")"
check "move removed the source" "" "$(rclone lsf "$R/copy/hello.txt" 2> /dev/null || true)"

rclone purge "$R"
check "bucket purged" 0 "$(rclone lsd aether: | grep -c compat-rclone || true)"
