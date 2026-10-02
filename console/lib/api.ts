// Client for the admin API (`/api/v1`) on the same origin. Requests carry the
// session cookie and the console header the server requires on writes.

export type Grant = { read: boolean; write: boolean; owner: boolean };

export type Key = {
  access_key_id: string;
  name: string;
  created_at: string;
  admin: boolean;
  can_create_buckets: boolean;
  disabled: boolean;
  buckets: Record<string, Grant>;
};

export type CreatedKey = Key & { secret_key: string };

export type BucketKey = Grant & { access_key_id: string; name: string };

export type Bucket = {
  name: string;
  created_at: string;
  objects: number;
  bytes: number;
  key_count: number;
  quota: Quota;
  keys?: BucketKey[];
  cors?: CorsRule[];
};

/** `null` is no limit. */
export type Quota = { max_bytes: number | null; max_objects: number | null };

export type CorsRule = {
  id?: string | null;
  allowed_origins: string[];
  allowed_methods: string[];
  allowed_headers: string[];
  expose_headers: string[];
  max_age_secs?: number | null;
};

export type ScrubReport = {
  started_at: number;
  finished_at: number | null;
  objects_checked: number;
  blocks_checked: number;
  bytes_checked: number;
  corrupt: { bucket: string; key: string; block_id: string; problem: string }[];
};

export type Status = {
  version: string;
  uptime_secs: number;
  buckets: number;
  objects: number;
  bytes: number;
  blocks: number;
  access_keys: number;
  disk_available_bytes: number | null;
  disk_total_bytes: number | null;
  scrub: ScrubReport | null;
  data_dir: string;
  block_size: number;
  inline_threshold: number;
  gc_pending_blocks: number;
  gc_grace_secs: number;
  metadata_latency_ms: number;
  s3_endpoint: string;
  s3_endpoint_configured: boolean;
  region: string;
  admin_tls: boolean;
  /** Fingerprint of the master key new objects are encrypted with; null when off. */
  encryption_key: string | null;
};

export type Session = {
  access_key_id: string;
  name: string;
  root: boolean;
  root_access_key_id: string;
};

export type Health = { status: "ready" | "unavailable"; version: string; tls: boolean };

export type ObjectItem = { key: string; size: number; etag: string; last_modified: string };

export type ObjectList = {
  prefix: string;
  objects: ObjectItem[];
  prefixes: string[];
  continuation: string | null;
};

export type ObjectDetail = ObjectItem & {
  content_type: string | null;
  content_encoding: string | null;
  content_disposition: string | null;
  content_language: string | null;
  cache_control: string | null;
  expires: string | null;
  user_metadata: Record<string, string>;
  tags: Record<string, string>;
  checksums: Record<string, string>;
  encrypted: boolean;
  parts: number;
  storage: "inline" | "blocks";
  blocks: number;
};

export type ShareLink = { url: string; expires_at: string };

export type StatsRange = "1h" | "24h" | "7d";

export type StatsPoint = {
  t: number;
  reads: number;
  writes: number;
  lists: number;
  other: number;
  p50_ms: number | null;
  p99_ms: number | null;
};

export type Stats = {
  range: StatsRange;
  now: number;
  start: number;
  history_start: number | null;
  point_secs: number;
  bar_secs: number;
  points: StatsPoint[];
  bars: { start: number; end: number; bytes_in: number; bytes_out: number; covered: boolean }[];
  current: { requests_per_sec: number; p50_ms: number | null; p99_ms: number | null };
  day: { since: number | null; requests: number; client_errors: number; server_errors: number };
  objects_change: { since: number; from: number; to: number } | null;
  objects: { t: number; objects: number }[];
};

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export const CONSOLE_HEADER = { "x-aether-console": "1" };
/** Marks timed refreshes, which do not count as use of the session. */
export const BACKGROUND_HEADER = { "x-aether-background": "1" };

/** Signed out (or the session expired): back to the sign-in page. */
export function toSignIn() {
  if (typeof window === "undefined" || window.location.pathname.startsWith("/login")) return;
  const next = window.location.pathname + window.location.search;
  // Called outside components, where the router is out of reach; a full page
  // load also drops the signed-in state.
  // eslint-disable-next-line @next/next/no-location-assign-relative-destination
  window.location.assign(`/login/?next=${encodeURIComponent(next)}`);
}

export async function api<T>(path: string, init: RequestInit & { json?: unknown } = {}): Promise<T> {
  const { json, headers, ...rest } = init;
  let response: Response;
  try {
    response = await fetch(`/api/v1${path}`, {
      credentials: "same-origin",
      ...rest,
      headers: {
        ...CONSOLE_HEADER,
        ...(json === undefined ? {} : { "content-type": "application/json" }),
        ...headers,
      },
      body: json === undefined ? rest.body : JSON.stringify(json),
    });
  } catch {
    throw new ApiError(0, "NetworkError", "The server did not answer. Is it running?");
  }
  if (response.status === 401 && !path.startsWith("/login")) {
    toSignIn();
  }
  const text = await response.text();
  const body = text ? safeJson(text) : null;
  if (!response.ok) {
    const error = (body as { error?: { code?: string; message?: string } } | null)?.error;
    throw new ApiError(response.status, error?.code ?? "Error", error?.message ?? response.statusText);
  }
  return body as T;
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

const enc = encodeURIComponent;

export const paths = {
  bucket: (name: string) => `/buckets/${enc(name)}`,
  objects: (bucket: string, prefix: string, continuation?: string | null, maxKeys = 200) =>
    `/buckets/${enc(bucket)}/objects?prefix=${enc(prefix)}&delimiter=%2F&max_keys=${maxKeys}` +
    (continuation ? `&continuation=${enc(continuation)}` : ""),
  object: (bucket: string, key: string) => `/buckets/${enc(bucket)}/object?key=${enc(key)}`,
  download: (bucket: string, key: string) => `/api/v1/buckets/${enc(bucket)}/object/download?key=${enc(key)}`,
  share: (bucket: string) => `/buckets/${enc(bucket)}/object/share`,
  uploads: (bucket: string) => `/buckets/${enc(bucket)}/uploads`,
  upload: (bucket: string, id: string, key: string) => `/buckets/${enc(bucket)}/uploads/${enc(id)}?key=${enc(key)}`,
  part: (bucket: string, id: string, n: number, key: string) =>
    `/api/v1/buckets/${enc(bucket)}/uploads/${enc(id)}/parts/${n}?key=${enc(key)}`,
  complete: (bucket: string, id: string) => `/buckets/${enc(bucket)}/uploads/${enc(id)}/complete`,
  key: (id: string) => `/keys/${enc(id)}`,
  grant: (id: string, bucket: string) => `/keys/${enc(id)}/buckets/${enc(bucket)}`,
};
