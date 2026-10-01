# Aether

S3-compatible object storage written in Rust, for teams that run their own
servers or containers and need object storage for internal applications.

> **Status: early development.** Single-node object storage works with the AWS
> SDKs, the AWS CLI and boto3 using their default settings: multipart uploads,
> server-side copies, per-application access keys, HTTPS, Prometheus metrics and
> a Docker image. Compatibility testing in CI is next. See the
> [roadmap](docs/ROADMAP.md).

No AWS account or cloud service is involved: Aether runs on your servers, and
"AWS" below only refers to standard S3 client tools.

- [Design](docs/DESIGN.md): architecture and the decisions behind it
- [Roadmap](docs/ROADMAP.md): what is done and what comes next

## Run it

### With Docker

```sh
cp .env.example .env      # set the root access key and a long random secret
docker compose up -d
```

[`compose.yaml`](compose.yaml) builds the image, keeps data in a named volume,
publishes the S3 API on port 9000 and the admin API and metrics on the host's
loopback interface only (127.0.0.1:9001). The image runs as a non-root user,
has a health check, and shuts down cleanly on `docker stop`. Run the CLI inside
the container: `docker compose exec aether aether status`.

### From source

Requires Rust 1.96 or newer.

```sh
cd aether_backend
cargo build --release

AETHER_S3__ACCESS_KEY=admin \
AETHER_S3__SECRET_KEY=change-me-please \
AETHER_STORAGE__DATA_DIR=/var/lib/aether \
./target/release/aether
```

The S3 API listens on port 9000 and the admin API on `127.0.0.1:9001`. The root
key set here can do everything; give each application its own key (below). All
settings are listed in
[`aether.example.toml`](aether_backend/aether.example.toml); copy it to
`aether.toml` or pass `--config <file>`. Every setting can also be given as an
environment variable: `AETHER_<SECTION>__<KEY>`, e.g. `AETHER_TLS__CERT_FILE`.

### HTTPS

Set `tls.cert_file` and `tls.key_file` (PEM) to serve the S3 API over HTTPS;
`tls.admin = true` does the same for the admin API. Renewed certificate files
are picked up within a minute, without a restart. For a self-signed test
certificate:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 365 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE" -keyout tls.key -out tls.crt
```

Clients then need to trust it: `aws --ca-bundle tls.crt`, `aether --ca-cert tls.crt`.

## Give each application its own key

Run these on the server host: the `aether` commands read the same config file
and talk to the admin API with the root key. Elsewhere, pass `--url`,
`--access-key` and `--secret-key` (or `AETHER_ADMIN_URL`,
`AETHER_ADMIN_ACCESS_KEY`, `AETHER_ADMIN_SECRET_KEY`).

```sh
aether bucket create invoices
aether key create "billing service" --grant invoices=rw
# Access key id: AK64OA2XIS00HEL7LJ95
# Secret key:    kC0ZGtTTxQeHxEThsiwFD/rHagA4UrpgDhkLVZrR
# Save the secret key now: it cannot be shown again.

aether key list
aether key grant AK64OA2XIS00HEL7LJ95 reports r   # r: read, w: write, o: owner
aether key disable AK64OA2XIS00HEL7LJ95           # takes effect immediately
aether bucket show invoices                       # usage and which keys have access
aether status
```

Other options: `--admin` for full access, `--create-buckets` for keys that create
(and own) their buckets, and `--import-id`/`--import-secret` to keep an
application's existing credentials when migrating from another S3 store. Every
command takes `--json`. The underlying JSON API is described in
[DESIGN.md](docs/DESIGN.md#admin-api).

## Use it

Point any S3 client at the endpoint with path-style addressing. The examples use
the root key; applications use their own.

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
- One node only: no replication yet, so back up the data directory (see the
  roadmap for clustering).

## Monitor it

- **Health:** `GET /_aether/health/live` (the process is up) and
  `GET /_aether/health/ready` (the metadata store answers) on the S3 port, for
  load balancers and orchestrators. `aether health` does the same check for
  container health checks.
- **Metrics:** Prometheus format at `GET /metrics` on the admin port, without
  credentials: requests by operation and status, latency, bytes in and out,
  per-bucket objects and bytes, disk space, garbage collection and scrubbing.
- **Logs:** one access log line per request (method, path, status, S3
  operation, access key, duration), as text or JSON (`log.format = "json"`).
  Every response carries a request id (`x-amz-request-id`, which SDKs include in
  error messages) that also appears on every log line of that request.
- **Data integrity:** every block is re-read and checked against its checksum
  once a week by default (rate-limited). `aether status` shows the last result;
  `aether scrub` runs one now. Damaged blocks are logged as errors and counted in
  `aether_scrub_corrupt_blocks_total`.

## Develop

```sh
cd aether_backend
cargo test      # unit tests and end-to-end tests with the AWS Rust SDK
cargo clippy --all-targets
```
