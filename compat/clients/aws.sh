#!/usr/bin/env bash
# The AWS CLI v2 against Aether: high-level `aws s3` transfers (multipart
# uploads, multipart copies, sync, presigned URLs) and `aws s3api` calls.
set -euo pipefail
source "$(dirname "$0")/common.sh"

export AWS_ACCESS_KEY_ID=$COMPAT_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$COMPAT_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true AWS_CONFIG_FILE=/dev/null
# No pager, and a stalled request fails within seconds instead of being
# retried for minutes.
export AWS_PAGER="" AWS_MAX_ATTEMPTS=1
# `command`: without AWS_CLI, a bare `aws` here would call this function again.
aws() {
    command "${AWS_CLI:-aws}" --endpoint-url "$COMPAT_ENDPOINT" --cli-connect-timeout 10 --cli-read-timeout 30 "$@"
}
B=compat-aws

make_tree
aws s3 mb "s3://$B" > /dev/null
check "bucket is listed" "$B" "$(aws s3api list-buckets --query 'Buckets[].Name' --output text)"

# Default thresholds: large.bin (21 MiB) goes up in 8 MiB parts.
aws s3 cp tree/large.bin "s3://$B/large.bin" --only-show-errors
aws s3 cp "s3://$B/large.bin" large.out --only-show-errors
cmp tree/large.bin large.out && echo "ok: multipart upload round trip"
check "multipart ETag has a part count" "-3" \
    "$(aws s3api head-object --bucket $B --key large.bin --query ETag --output text | grep -o -- '-[0-9]*')"

aws s3 sync tree "s3://$B/tree" --only-show-errors
aws s3 sync "s3://$B/tree" synced --only-show-errors
diff -r tree synced && echo "ok: sync up and down"
check "recursive listing" 6 "$(aws s3 ls "s3://$B/tree/" --recursive | wc -l)"
check "nothing left to sync" "" "$(aws s3 sync tree "s3://$B/tree" --dryrun)"
rm tree/hello.txt
aws s3 sync tree "s3://$B/tree" --delete --only-show-errors
check "sync --delete removed the key" 5 "$(aws s3 ls "s3://$B/tree/" --recursive | wc -l)"

# Server-side copies: CopyObject below the threshold, UploadPartCopy above.
aws s3 cp "s3://$B/tree/sub/one-mib.bin" "s3://$B/copies/one-mib.bin" --only-show-errors
aws s3 cp "s3://$B/large.bin" "s3://$B/copies/large.bin" --only-show-errors
aws s3 cp "s3://$B/copies/large.bin" large-copy.out --only-show-errors
cmp tree/large.bin large-copy.out && echo "ok: multipart server-side copy"

aws s3api put-object --bucket $B --key meta.txt --body tree/sub/deeper/just-over-inline.bin \
    --content-type text/plain --metadata project=aether,owner=ci --cache-control no-cache > /dev/null
check "content type" text/plain "$(aws s3api head-object --bucket $B --key meta.txt --query ContentType --output text)"
check "user metadata" "ci aether" \
    "$(aws s3api head-object --bucket $B --key meta.txt --query 'Metadata.[owner,project]' --output text | tr '\t' ' ')"
aws s3api get-object --bucket $B --key tree/sub/one-mib.bin --range bytes=100-199 range.out > /dev/null
check "ranged GET" "$(head -c 200 tree/sub/one-mib.bin | tail -c 100 | md5sum)" "$(md5sum < range.out)"

check "paginated listing" 9 \
    "$(aws s3api list-objects-v2 --bucket $B --page-size 2 --query 'length(Contents)' --output json)"
check "delimiter listing" "copies/ tree/" \
    "$(aws s3api list-objects-v2 --bucket $B --delimiter / --query 'CommonPrefixes[].Prefix' --output text | tr '\t' ' ')"

url=$(aws s3 presign "s3://$B/meta.txt" --expires-in 300)
curl -sf "$url" -o presigned.out
cmp presigned.out tree/sub/deeper/just-over-inline.bin && echo "ok: presigned GET"

if aws s3api put-object --bucket $B --key meta.txt --body tree/empty --if-none-match '*' 2> conditional.err; then
    echo "FAIL: If-None-Match: * overwrote an object" >&2
    exit 1
fi
grep -q PreconditionFailed conditional.err && echo "ok: conditional write refused"

aws s3 rm "s3://$B" --recursive --only-show-errors
aws s3 rb "s3://$B" > /dev/null
check "bucket removed" "" "$(aws s3api list-buckets --query 'Buckets[].Name' --output text)"
