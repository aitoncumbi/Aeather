# Aether

S3-compatible object storage written in Rust, for teams that run their own
servers or containers and need object storage for internal applications.

> **Status: early development.** Single-node object storage, including multipart
> uploads and server-side copies, works with the AWS SDKs, the AWS CLI and boto3
> using their default settings. Per-application access keys and the admin API are
> next. See the [roadmap](docs/ROADMAP.md).

No AWS account or cloud service is involved: Aether runs on your servers, and
"AWS" below only refers to standard S3 client tools.

- [Design](docs/DESIGN.md): architecture and the decisions behind it
- [Roadmap](docs/ROADMAP.md): what is done and what comes next

## Run it

Requires Rust 1.96 or newer.

```sh
cd aether_backend
cargo build --release

AETHER_S3__ACCESS_KEY=admin \
AETHER_S3__SECRET_KEY=change-me-please \
AETHER_STORAGE__DATA_DIR=/var/lib/aether \
./target/release/aether
```

The S3 API listens on port 9000. All settings are listed in
[`aether.example.toml`](aether_backend/aether.example.toml); copy it to
`aether.toml` or pass `--config <file>`.

Health checks for load balancers and orchestrators:

- `GET /_aether/health/live`: the process is up
- `GET /_aether/health/ready`: the metadata store is answering

## Use it

Point any S3 client at the endpoint with path-style addressing.

```sh
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=change-me-please AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://reports
aws --endpoint-url http://localhost:9000 s3 cp ./q3.pdf s3://reports/2026/q3.pdf
aws --endpoint-url http://localhost:9000 s3 ls s3://reports --recursive
```

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="change-me-please",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
s3.put_object(Bucket="reports", Key="hello.txt", Body=b"hello")
```

Current limitations:

- **Only Signature Version 4 is accepted.** AWS CLI v1 creates SigV2 presigned
  URLs by default; set `s3.signature_version = s3v4` in its config.
- One root key for now; per-application keys come with the admin API.

## Develop

```sh
cd aether_backend
cargo test      # unit tests and end-to-end tests with the AWS Rust SDK
cargo clippy --all-targets
```
