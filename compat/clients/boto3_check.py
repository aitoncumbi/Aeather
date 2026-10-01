"""boto3 against Aether: managed transfers, the multipart API, checksums,
conditional writes, batch deletes and presigned URLs."""

import hashlib
import io
import os
import urllib.request

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1024 * 1024
BUCKET = "compat-boto3"

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["COMPAT_ENDPOINT"],
    aws_access_key_id=os.environ["COMPAT_ACCESS_KEY"],
    aws_secret_access_key=os.environ["COMPAT_SECRET_KEY"],
    region_name="us-east-1",
    # Without signature_version, boto3 presigns URLs with the legacy SigV2,
    # which Aether accepts only when s3.allow_sig_v2 is set.
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
)
os.makedirs(os.environ["WORKDIR"], exist_ok=True)
os.chdir(os.environ["WORKDIR"])


def ok(what):
    print(f"ok: {what}")


def error_code(call, **kwargs):
    try:
        call(**kwargs)
    except ClientError as e:
        return e.response["Error"]["Code"]
    raise AssertionError(f"{call.__name__} succeeded")


s3.create_bucket(Bucket=BUCKET)

# Plain objects, with the checksum boto3 now sends by default.
s3.put_object(Bucket=BUCKET, Key="note.txt", Body=b"hello", ContentType="text/plain", Metadata={"team": "storage"})
got = s3.get_object(Bucket=BUCKET, Key="note.txt", ChecksumMode="ENABLED")
assert got["Body"].read() == b"hello"
assert got["ContentType"] == "text/plain" and got["Metadata"] == {"team": "storage"}
assert got.get("ChecksumCRC32") or got.get("ChecksumCRC64NVME"), "stored checksum is returned"
assert s3.get_object(Bucket=BUCKET, Key="note.txt", Range="bytes=1-3")["Body"].read() == b"ell"
ok("put, get, ranged get, metadata and checksum")

# Managed transfers with 5 MiB parts.
data = os.urandom(17 * MIB + 123)
with open("big.bin", "wb") as f:
    f.write(data)
transfer = TransferConfig(multipart_threshold=5 * MIB, multipart_chunksize=5 * MIB)
s3.upload_file("big.bin", BUCKET, "big.bin", Config=transfer)
s3.download_file(BUCKET, "big.bin", "big.out", Config=transfer)
with open("big.out", "rb") as f:
    assert hashlib.sha256(f.read()).digest() == hashlib.sha256(data).digest()
head = s3.head_object(Bucket=BUCKET, Key="big.bin")
assert head["ContentLength"] == len(data) and head["ETag"].endswith('-4"'), head["ETag"]
ok("multipart upload_file / download_file")

s3.copy({"Bucket": BUCKET, "Key": "big.bin"}, BUCKET, "big-copy.bin", Config=transfer)
buf = io.BytesIO()
s3.download_fileobj(BUCKET, "big-copy.bin", buf)
assert buf.getvalue() == data
ok("managed copy (UploadPartCopy)")

# The low-level multipart API.
upload = s3.create_multipart_upload(Bucket=BUCKET, Key="parts.bin")["UploadId"]
part1 = s3.upload_part(Bucket=BUCKET, Key="parts.bin", UploadId=upload, PartNumber=1, Body=data[: 5 * MIB])
part2 = s3.upload_part(Bucket=BUCKET, Key="parts.bin", UploadId=upload, PartNumber=2, Body=b"tail")
listed = s3.list_parts(Bucket=BUCKET, Key="parts.bin", UploadId=upload)["Parts"]
assert [p["Size"] for p in listed] == [5 * MIB, 4]
assert s3.list_multipart_uploads(Bucket=BUCKET)["Uploads"][0]["UploadId"] == upload
s3.complete_multipart_upload(
    Bucket=BUCKET,
    Key="parts.bin",
    UploadId=upload,
    MultipartUpload={
        "Parts": [{"PartNumber": 1, "ETag": part1["ETag"]}, {"PartNumber": 2, "ETag": part2["ETag"]}]
    },
)
assert s3.get_object(Bucket=BUCKET, Key="parts.bin", PartNumber=2)["Body"].read() == b"tail"
abandoned = s3.create_multipart_upload(Bucket=BUCKET, Key="abandoned")["UploadId"]
s3.abort_multipart_upload(Bucket=BUCKET, Key="abandoned", UploadId=abandoned)
assert "Uploads" not in s3.list_multipart_uploads(Bucket=BUCKET)
ok("multipart API, part reads and abort")

# Listing with a paginator.
for i in range(25):
    s3.put_object(Bucket=BUCKET, Key=f"many/{i:02}", Body=b"x")
pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="many/", PaginationConfig={"PageSize": 10})
keys = [o["Key"] for page in pages for o in page.get("Contents", [])]
assert keys == [f"many/{i:02}" for i in range(25)], keys
listing = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/")
assert [p["Prefix"] for p in listing["CommonPrefixes"]] == ["many/"]
ok("paginated and delimited listing")

# Conditional writes.
assert error_code(s3.put_object, Bucket=BUCKET, Key="note.txt", Body=b"x", IfNoneMatch="*") == "PreconditionFailed"
etag = s3.head_object(Bucket=BUCKET, Key="note.txt")["ETag"]
s3.put_object(Bucket=BUCKET, Key="note.txt", Body=b"hello again", IfMatch=etag)
assert error_code(s3.put_object, Bucket=BUCKET, Key="note.txt", Body=b"x", IfMatch=etag) == "PreconditionFailed"
ok("conditional writes")

# Presigned URLs.
url = s3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": "presigned"}, ExpiresIn=300)
urllib.request.urlopen(urllib.request.Request(url, data=b"via presigned PUT", method="PUT")).read()
url = s3.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": "presigned"}, ExpiresIn=300)
assert urllib.request.urlopen(url).read() == b"via presigned PUT"
ok("presigned PUT and GET")

# Batch delete, then the bucket.
assert error_code(s3.delete_bucket, Bucket=BUCKET) == "BucketNotEmpty"
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
    objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objects:
        result = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects})
        assert not result.get("Errors"), result.get("Errors")
s3.delete_bucket(Bucket=BUCKET)
assert error_code(s3.head_bucket, Bucket=BUCKET) == "404"
ok("batch delete and bucket removal")
