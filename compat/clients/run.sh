#!/usr/bin/env bash
# Drives Aether with real S3 clients: the AWS CLI v2, boto3 and rclone.
#
#   compat/clients/run.sh               run every client
#   compat/clients/run.sh aws rclone    run some of them
#
# Each client signs with an ordinary access key (not root) that may create
# buckets, and works in its own bucket. Environment: AETHER (the binary),
# AWS_CLI, PYTHON (with boto3), RCLONE, PORT, KEEP_LOG (keep the server log),
# CLIENT_TIMEOUT (seconds each client may take; default 600),
# COMPAT_LOG_LEVEL and COMPAT_ACCESS_LOG=true (more detail in that log).
set -euo pipefail
cd "$(dirname "$0")/../.."
source compat/lib.sh

clients=("$@")
if [ ${#clients[@]} -eq 0 ]; then clients=(aws boto3 rclone); fi

start_server "${PORT:-9400}"
export COMPAT_ENDPOINT=$S3_URL
export COMPAT_ACCESS_KEY=COMPATTESTKEY0000001
export COMPAT_SECRET_KEY=compat-test-secret-key-0123456789abcdefgh
"$AETHER" key create "client compatibility" --create-buckets \
    --import-id "$COMPAT_ACCESS_KEY" --import-secret "$COMPAT_SECRET_KEY" > /dev/null

failed=()
for client in "${clients[@]}"; do
    echo "=== $client"
    case $client in
        aws) script=(bash compat/clients/aws.sh) ;;
        boto3) script=("${PYTHON:-python3}" compat/clients/boto3_check.py) ;;
        rclone) script=(bash compat/clients/rclone.sh) ;;
        *) echo "unknown client: $client" >&2; exit 2 ;;
    esac
    # A hung client fails here, with the server log, rather than stalling CI.
    if WORKDIR="$WORK/$client" timeout "${CLIENT_TIMEOUT:-600}" "${script[@]}"; then
        echo "=== $client: ok"
    else
        echo "=== $client: FAILED"
        failed+=("$client")
    fi
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "Failed: ${failed[*]}; server log:"
    tail -n 50 "$WORK/server.log"
    exit 1
fi
