# Aether

S3-compatible object storage written in Rust, for teams that run their own
servers or containers and need object storage for internal applications.

> **Status: v1 milestones complete, not yet proven in production.** A single
> node works with the AWS SDKs, the AWS CLI, boto3 and rclone: multipart uploads,
> server-side copies, per-application access keys, HTTPS, Prometheus metrics, a
> Docker image and a web console. CI drives those clients against a live server
> and runs 321 of Ceph's s3-tests. See the [roadmap](docs/ROADMAP.md) for what
> comes next.

No AWS account or cloud service is involved: Aether runs on your servers, and
"AWS" below only refers to standard S3 client tools.

- [Design](docs/DESIGN.md): architecture and the decisions behind it
- [Roadmap](docs/ROADMAP.md): what is done and what comes next
- [Benchmarks](docs/BENCHMARKS.md): baseline numbers and how to reproduce them

## Run it

### With Docker

```sh
cp .env.example .env      # set the root access key and a long random secret
docker compose up -d
```

[`compose.yaml`](compose.yaml) builds the image, keeps data in a named volume,
publishes the S3 API on port 9000 and the admin API, web console and metrics on
the host's loopback interface only (127.0.0.1:9001). The image runs as a non-root user,
has a health check, and shuts down cleanly on `docker stop`. Run the CLI inside
the container: `docker compose exec aether aether status`.

### From source

Requires Rust 1.96 or newer, and Node.js 20.9 or newer for the web console.

```sh
(cd console && npm ci && npm run build)   # optional: the web console
cd aether_backend
cargo build --release                      # embeds console/out when it exists

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

### Encryption at rest

```sh
openssl rand -base64 32      # store it apart from the data directory
AETHER_ENCRYPTION__MASTER_KEY=<that value> ./target/release/aether
```

With a master key, every new object is encrypted (SSE-S3, AES-256-GCM); objects
written before stay as they were. Access-key secrets in the metadata are
encrypted too, so a copy of the database alone does not reveal working keys. **Losing the key loses the data**, so back it
up separately. To rotate, set a new `encryption.master_key` and move the old one
to `encryption.previous_keys`. The server refuses to start without every key
that has encrypted stored data.

Clients can also bring their own key per object (SSE-C, with the
`x-amz-server-side-encryption-customer-*` headers). Aether stores only a salted
HMAC of it, so the key is needed for every read. Since the key is in the request
headers, SSE-C needs HTTPS; behind a proxy that terminates TLS, set
`s3.allow_sse_c_over_http = true`. The web console cannot download these
objects.

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

## Web console

Open the admin port in a browser, `http://127.0.0.1:9001/`, and sign in with the
root key or another admin key. The console shows usage and request activity,
creates and deletes buckets, browses, uploads, downloads and deletes objects,
makes presigned share links, and manages keys and their bucket permissions.

The admin port listens on loopback by default. From another machine, use an SSH
tunnel (`ssh -L 9001:127.0.0.1:9001 server`, then open `http://127.0.0.1:9001/`)
or serve the admin API over HTTPS (`tls.admin = true`) on a reachable address.

- Sign-in sessions end on sign-out, after 15 minutes idle (automatic page
  refreshes do not count) or 7 days, when the server restarts, and immediately
  when the key is disabled or deleted.
- Share links are presigned S3 URLs: whoever has one can download the object
  until it expires, from the S3 port. They use the host name the console was
  opened with; when clients reach the S3 API at another address (a load
  balancer, a reverse proxy), set `s3.public_url = "https://s3.example.com"`.
- Request charts cover what the server has seen since it started; use the
  Prometheus metrics for long-term history.
- `admin.console = false` turns the console off; the admin API is unchanged.

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
aether bucket quota invoices --max-bytes 50GiB    # also --max-objects; `none` or --clear removes
aether bucket versioning invoices enable          # keep every version; `suspend` to stop
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
    # s3v4: otherwise boto3 presigns URLs with the legacy SigV2.
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
s3.put_object(Bucket="reports", Key="hello.txt", Body=b"hello")
```

Current limitations:

- **Signature Version 4 only, by default.** boto3 and AWS CLI v1 presign URLs
  with the deprecated SigV2 unless told otherwise: use
  `signature_version="s3v4"` as above (AWS CLI v1: `s3.signature_version = s3v4`
  in its config). `s3.allow_sig_v2 = true` accepts SigV2 for clients that cannot
  be changed.
- **Path-style addressing by default** (`http://host:9000/bucket/key`). For
  virtual-hosted style (`http://bucket.s3.example.com/key`), set
  `s3.domains = ["s3.example.com"]` with wildcard DNS for `*.s3.example.com`
  (and a wildcard certificate with TLS).
- **Not supported yet:** SSE-KMS; lifecycle
  transitions (Aether has one storage class); ACLs and bucket policies
  (per-key bucket permissions replace them); object lock; browser POST uploads.
  Requests that need these fail with `NotImplemented` rather than being ignored.
- One node only: no replication yet, so back up the data directory (see the
  roadmap for clustering).

## Monitor it

- **Health:** `GET /_aether/health/live` (the process is up) and
  `GET /_aether/health/ready` (the metadata store answers) on the S3 port, for
  load balancers and orchestrators. `aether health` does the same check for
  container health checks.
- **Metrics:** Prometheus format at `GET /metrics` on the admin port, without
  credentials: requests by operation and status, latency, bytes in and out,
  per-bucket objects, bytes and quotas, disk space, garbage collection and scrubbing.
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
cargo build --release
```

The console is a Next.js app in [`console/`](console). With a server running,
`npm run dev` serves it with hot reloading at http://localhost:3000 and forwards
`/api` to the admin API (`AETHER_ADMIN_URL`, default `http://127.0.0.1:9001`).
Its end-to-end tests start a release build with its own data and drive the
embedded console in Chromium:

```sh
cd console
npm ci && npx playwright install chromium
npm run typecheck
npm run build && (cd ../aether_backend && cargo build --release)
npm run e2e
```

Compatibility checks run a release build against real clients (as CI does):

```sh
compat/clients/run.sh                          # AWS CLI v2, boto3 and rclone
S3TESTS_DIR=~/src/s3-tests compat/s3-tests/run.sh   # Ceph s3-tests in the pass list
S3TESTS_DIR=~/src/s3-tests compat/s3-tests/run.sh --update   # after fixing more of them
```

The s3-tests runner needs a [ceph/s3-tests](https://github.com/ceph/s3-tests)
checkout with its requirements and `pytest-timeout` installed; the revision CI
uses is in [`.github/workflows/ci.yml`](.github/workflows/ci.yml). Benchmarks:
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## License

[MIT](LICENSE)
