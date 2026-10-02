"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useRef, useState, type DragEvent } from "react";

import { api, ApiError, paths, type Bucket, type ObjectDetail, type ObjectList, type ShareLink } from "@/lib/api";
import { baseName, bytes, count, dateTime, extension, kind } from "@/lib/format";
import { copyText, useApi } from "@/lib/hooks";
import { upload, type Progress, type UploadHandle } from "@/lib/upload";
import {
  BucketIcon,
  ChevronsUpDownIcon,
  CloseIcon,
  CopyIcon,
  DownloadIcon,
  FileIcon,
  FolderIcon,
  LinkIcon,
  SearchIcon,
  TrashIcon,
  UploadIcon,
} from "./icons";
import { useShell } from "./Shell";
import { ConfirmDialog, CopyButton, ErrorPanel, useToast } from "./ui";

type Transfer = {
  id: number;
  name: string;
  key: string;
  progress: Progress;
  started: number;
  /** When `progress` last changed. */
  updated: number;
  state: "running" | "done" | "failed";
  error?: string;
  handle: UploadHandle;
};

export function Objects() {
  const router = useRouter();
  const params = useSearchParams();
  const { refreshStatus } = useShell();
  const buckets = useApi<Bucket[]>("/buckets");
  const bucket = params.get("bucket") ?? buckets.data?.[0]?.name ?? null;
  const prefix = params.get("prefix") ?? "";
  const selected = params.get("key");

  const go = useCallback(
    (next: { bucket?: string | null; prefix?: string; key?: string | null }) => {
      const b = next.bucket !== undefined ? next.bucket : bucket;
      const q = new URLSearchParams();
      if (b) q.set("bucket", b);
      const p = next.prefix !== undefined ? next.prefix : prefix;
      if (p) q.set("prefix", p);
      const k = next.key !== undefined ? next.key : selected;
      if (k) q.set("key", k);
      router.replace(`/objects/?${q.toString()}`, { scroll: false });
    },
    [bucket, prefix, selected, router],
  );

  if (buckets.error) return <ErrorPanel message={buckets.error.message} onRetry={buckets.reload} />;
  if (buckets.data && buckets.data.length === 0) {
    return (
      <div className="page">
        <div className="empty">
          <div className="empty-icon">
            <BucketIcon size={18} />
          </div>
          <span style={{ fontWeight: 500, color: "var(--text)" }}>No buckets yet</span>
          <span style={{ fontSize: 13 }}>Objects live in buckets. Create one first.</span>
          <Link href="/buckets/" className="btn btn-sm">
            Go to buckets
          </Link>
        </div>
      </div>
    );
  }
  if (!bucket) return <div className="page" aria-busy="true" />;
  return (
    <Browser
      key={bucket}
      bucket={bucket}
      buckets={buckets.data ?? []}
      prefix={prefix}
      selected={selected}
      go={go}
      onChanged={refreshStatus}
    />
  );
}

function Browser({
  bucket,
  buckets,
  prefix,
  selected,
  go,
  onChanged,
}: {
  bucket: string;
  buckets: Bucket[];
  prefix: string;
  selected: string | null;
  go: (next: { bucket?: string | null; prefix?: string; key?: string | null }) => void;
  onChanged: () => void;
}) {
  const notify = useToast();
  const [filter, setFilter] = useState("");
  const [query, setQuery] = useState("");
  useEffect(() => {
    const timer = window.setTimeout(() => setQuery(filter), 250);
    return () => window.clearTimeout(timer);
  }, [filter]);
  // Reset during render when the folder changes (not in an effect, which would render twice).
  const [filterPrefix, setFilterPrefix] = useState(prefix);
  if (filterPrefix !== prefix) {
    setFilterPrefix(prefix);
    setFilter("");
  }

  const first = useApi<ObjectList>(paths.objects(bucket, prefix + query));
  const [more, setMore] = useState<ObjectList[]>([]);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreFor, setMoreFor] = useState(first.data);
  if (moreFor !== first.data) {
    setMoreFor(first.data);
    setMore([]);
  }
  const pages = first.data ? [first.data, ...more] : [];
  const prefixes = pages.flatMap((p) => p.prefixes);
  const objects = pages.flatMap((p) => p.objects);
  const continuation = pages[pages.length - 1]?.continuation ?? null;

  async function loadMore() {
    if (!continuation) return;
    setLoadingMore(true);
    try {
      const page = await api<ObjectList>(paths.objects(bucket, prefix + query, continuation));
      setMore((m) => [...m, page]);
    } catch (e) {
      notify(e instanceof Error ? e.message : String(e), "error");
    } finally {
      setLoadingMore(false);
    }
  }

  // ---- uploads ----
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const nextId = useRef(0);
  const fileInput = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const reload = first.reload;

  function startUploads(files: FileList | File[]) {
    for (const file of Array.from(files)) {
      const id = (nextId.current += 1);
      const key = prefix + file.name;
      const update = (patch: Partial<Transfer>) => setTransfers((all) => all.map((t) => (t.id === id ? { ...t, ...patch } : t)));
      const handle = upload(bucket, key, file, (progress) => update({ progress, updated: Date.now() }));
      setTransfers((all) => [
        ...all,
        { id, name: file.name, key, progress: { sent: 0, total: file.size }, started: Date.now(), updated: Date.now(), state: "running", handle },
      ]);
      handle.done.then(
        () => {
          update({ state: "done" });
          reload();
          onChanged();
          window.setTimeout(() => setTransfers((all) => all.filter((t) => t.id !== id)), 4000);
        },
        (e: unknown) => {
          if (e instanceof ApiError && e.code === "Cancelled") {
            setTransfers((all) => all.filter((t) => t.id !== id));
          } else {
            update({ state: "failed", error: e instanceof Error ? e.message : String(e) });
          }
        },
      );
    }
  }

  function onDrop(e: DragEvent) {
    e.preventDefault();
    setDragging(false);
    if (e.dataTransfer.files.length) startUploads(e.dataTransfer.files);
  }

  const crumbs = [{ label: bucket, prefix: "" }].concat(
    prefix
      .split("/")
      .filter(Boolean)
      .map((segment, i, all) => ({ label: segment, prefix: `${all.slice(0, i + 1).join("/")}/` })),
  );
  const pageBytes = objects.reduce((a, o) => a + o.size, 0);

  return (
    <div className="page" style={{ gap: 18 }}>
      <div className="head">
        <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", minWidth: 0 }}>
          <BucketSwitcher bucket={bucket} buckets={buckets} onPick={(b) => go({ bucket: b, prefix: "", key: null })} />
          <nav aria-label="Prefix" style={{ display: "flex", alignItems: "center", gap: 2, flexWrap: "wrap" }}>
            {crumbs.map((c, i) => (
              <span key={c.prefix} style={{ display: "flex", alignItems: "center", gap: 2 }}>
                {i > 0 && <span className="sep" style={{ fontSize: 16, padding: "0 2px" }}>/</span>}
                {i === 0 ? (
                  crumbs.length > 1 && (
                    <button type="button" className="crumb" onClick={() => go({ prefix: "", key: null })}>
                      (root)
                    </button>
                  )
                ) : (
                  <button
                    type="button"
                    className="crumb"
                    aria-current={i === crumbs.length - 1 ? "page" : undefined}
                    onClick={() => go({ prefix: c.prefix, key: null })}
                  >
                    {c.label}
                  </button>
                )}
              </span>
            ))}
          </nav>
        </div>
        <div className="head-actions">
          <label className="field" style={{ minWidth: 220, flexGrow: 1 }}>
            <SearchIcon size={15} />
            <span className="sr-only">Filter this prefix by the start of the name</span>
            <input type="search" placeholder="Filter: name starts with" value={filter} onChange={(e) => setFilter(e.target.value)} />
          </label>
          <button type="button" className="btn btn-primary" style={{ fontWeight: 500 }} onClick={() => fileInput.current?.click()}>
            <UploadIcon />
            Upload
          </button>
          <input
            ref={fileInput}
            type="file"
            multiple
            hidden
            onChange={(e) => {
              if (e.target.files?.length) startUploads(e.target.files);
              e.target.value = "";
            }}
          />
        </div>
      </div>

      {transfers.map((t) => (
        <TransferRow key={t.id} transfer={t} onDismiss={() => setTransfers((all) => all.filter((x) => x.id !== t.id))} />
      ))}

      <div className="ruled split">
        <section
          aria-label="Objects in this prefix"
          className={`list${dragging ? " dropzone" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <div className="thead cols-objects">
            <span>Name</span>
            <span className="right">Size</span>
            <span className="hide-sm">Type</span>
            <span className="hide-sm">Last modified</span>
          </div>
          {first.error && <ErrorPanel message={first.error.message} onRetry={first.reload} />}
          {prefixes.map((p) => (
            <button key={p} type="button" className="trow cols-objects" style={{ minHeight: 48 }} onClick={() => go({ prefix: p, key: null })}>
              <span style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                <FolderIcon size={16} fill="#27272A" color="var(--muted)" style={{ flexShrink: 0 }} />
                <span className="mono ellipsis" style={{ fontSize: 13 }}>
                  {p.slice(prefix.length)}
                </span>
              </span>
              <span className="right muted">—</span>
              <span className="muted hide-sm" style={{ fontSize: 13 }}>
                Prefix
              </span>
              <span className="muted hide-sm" style={{ fontSize: 13 }}>
                —
              </span>
            </button>
          ))}
          {objects.map((o) => (
            <button
              key={o.key}
              type="button"
              className="trow cols-objects"
              style={{ minHeight: 48 }}
              aria-pressed={o.key === selected}
              onClick={() => go({ key: o.key })}
            >
              <span style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                <FileIcon size={16} color="var(--subtle)" style={{ flexShrink: 0 }} />
                <span className="mono ellipsis" style={{ fontSize: 13 }} title={o.key}>
                  {o.key.slice(prefix.length)}
                </span>
              </span>
              <span className="right tnum muted" style={{ fontSize: 13 }}>
                {bytes(o.size)}
              </span>
              <span className="muted hide-sm" style={{ fontSize: 13 }}>
                {kind(o.key)}
              </span>
              <span className="muted hide-sm ellipsis" style={{ fontSize: 13 }}>
                {dateTime(o.last_modified)}
              </span>
            </button>
          ))}
          {first.data && prefixes.length === 0 && objects.length === 0 && (
            <div className="empty">
              <div className="empty-icon">
                <UploadIcon size={18} />
              </div>
              <span style={{ fontWeight: 500, color: "var(--text)" }}>{query ? "Nothing starts with that" : "Nothing here yet"}</span>
              <span style={{ fontSize: 13 }}>{query ? "Filters match the start of names in this prefix." : "Upload files, or drop them on this list."}</span>
            </div>
          )}
          {continuation && (
            <div style={{ padding: "12px 24px 0" }}>
              <button type="button" className="btn btn-sm" onClick={loadMore} disabled={loadingMore}>
                {loadingMore ? "Loading…" : "Load more"}
              </button>
            </div>
          )}
          <div className="list-foot">
            <span>
              {count(prefixes.length)} prefix{prefixes.length === 1 ? "" : "es"} · {count(objects.length)} object{objects.length === 1 ? "" : "s"}
              {continuation ? " shown" : ""} · {bytes(pageBytes)}
            </span>
            <span>Sorted by key, as S3 lists them</span>
          </div>
        </section>

        {selected ? (
          <ObjectPanel
            key={selected}
            bucket={bucket}
            objectKey={selected}
            onDeleted={() => {
              go({ key: null });
              reload();
              onChanged();
            }}
          />
        ) : (
          <section aria-label="Object details" className="panel">
            <div className="empty">
              <div className="empty-icon">
                <FileIcon size={18} />
              </div>
              <span style={{ fontWeight: 500, color: "var(--text)" }}>No object selected</span>
              <span style={{ fontSize: 13, maxWidth: 260 }}>Pick an object to see its metadata, download it or share a presigned link.</span>
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function BucketSwitcher({ bucket, buckets, onPick }: { bucket: string; buckets: Bucket[]; onPick: (b: string) => void }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => !ref.current?.contains(e.target as Node) && setOpen(false);
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("mousedown", onDown);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("mousedown", onDown);
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        type="button"
        className="btn btn-surface"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={`Switch bucket, current: ${bucket}`}
        onClick={() => setOpen(!open)}
        style={{ padding: "0 10px 0 12px", fontFamily: "var(--mono)" }}
      >
        <BucketIcon size={15} color="var(--muted)" />
        {bucket}
        <ChevronsUpDownIcon size={14} color="var(--subtle)" />
      </button>
      {open && (
        <div className="menu" role="listbox" aria-label="Buckets">
          {buckets.map((b) => (
            <button
              key={b.name}
              type="button"
              role="option"
              aria-selected={b.name === bucket}
              aria-current={b.name === bucket}
              onClick={() => {
                setOpen(false);
                onPick(b.name);
              }}
            >
              {b.name}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function TransferRow({ transfer, onDismiss }: { transfer: Transfer; onDismiss: () => void }) {
  const { progress, state } = transfer;
  const secs = Math.max(0.5, (transfer.updated - transfer.started) / 1000);
  const speed = progress.sent / secs;
  const pct = progress.total ? (progress.sent / progress.total) * 100 : 100;
  return (
    <div className="upload" role="status">
      <div className="upload-icon">
        <UploadIcon />
      </div>
      <div style={{ flexGrow: 1, minWidth: 0, display: "flex", flexDirection: "column", gap: 6 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
          <span className="ellipsis">
            <span className="mono">{transfer.name}</span>
            <span className="subtle">
              {state === "done"
                ? " · uploaded"
                : state === "failed"
                  ? " · failed"
                  : progress.part
                    ? ` · multipart upload, part ${Math.min(progress.part.count, progress.part.done + 1)} of ${progress.part.count}`
                    : ""}
            </span>
          </span>
          <span className="muted tnum" style={{ whiteSpace: "nowrap" }}>
            {state === "failed" ? "" : `${bytes(progress.sent)} of ${bytes(progress.total)}${state === "running" ? ` · ${bytes(speed)}/s` : ""}`}
          </span>
        </div>
        {state === "failed" ? (
          <span className="error-text">{transfer.error}</span>
        ) : (
          <div style={{ height: 6, background: "#1e1e22" }}>
            <div style={{ width: `${pct.toFixed(1)}%`, height: 6, background: state === "done" ? "var(--ok)" : "var(--accent)" }} />
          </div>
        )}
      </div>
      {state === "running" ? (
        <button type="button" className="btn btn-sm" onClick={() => transfer.handle.cancel()}>
          Cancel
        </button>
      ) : (
        <button type="button" className="btn btn-quiet btn-icon btn-sm" aria-label="Dismiss" onClick={onDismiss}>
          <CloseIcon size={14} />
        </button>
      )}
    </div>
  );
}

const EXPIRIES = [
  { label: "1 hour", secs: 3600 },
  { label: "24 hours", secs: 86400 },
  { label: "7 days (maximum)", secs: 604800 },
];

function ObjectPanel({ bucket, objectKey, onDeleted }: { bucket: string; objectKey: string; onDeleted: () => void }) {
  const detail = useApi<ObjectDetail>(paths.object(bucket, objectKey));
  const notify = useToast();
  const [deleting, setDeleting] = useState(false);
  const [expires, setExpires] = useState(3600);
  const [link, setLink] = useState<ShareLink | null>(null);
  const [sharing, setSharing] = useState(false);

  if (detail.error) {
    return (
      <section aria-label="Object details" className="panel">
        <ErrorPanel message={detail.error.status === 404 ? "This object no longer exists." : detail.error.message} />
      </section>
    );
  }
  const o = detail.data;
  const name = baseName(objectKey);
  const ext = extension(name).toUpperCase().slice(0, 4) || "FILE";
  const uri = `s3://${bucket}/${objectKey}`;

  async function share() {
    setSharing(true);
    try {
      const created = await api<ShareLink>(paths.share(bucket), { method: "POST", json: { key: objectKey, expires_secs: expires } });
      setLink(created);
      if (await copyText(created.url)) notify("Link copied.");
    } catch (e) {
      notify(e instanceof Error ? e.message : String(e), "error");
    } finally {
      setSharing(false);
    }
  }

  const headers: [string, string][] = [];
  if (o) {
    const all: [string, string | null][] = [
      ["Cache-Control", o.cache_control],
      ["Content-Encoding", o.content_encoding],
      ["Content-Disposition", o.content_disposition],
      ["Content-Language", o.content_language],
      ["Expires", o.expires],
    ];
    for (const [name, value] of all) if (value) headers.push([name, value]);
  }
  const metadata = o ? Object.entries(o.user_metadata) : [];
  const tags = o ? Object.entries(o.tags ?? {}) : [];

  return (
    <section aria-label="Object details" className="panel" style={{ gap: 18 }}>
      <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
        <div className="empty-icon mono" style={{ fontSize: 11, fontWeight: 500, color: "var(--muted)" }}>
          {ext}
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
          <h2 className="mono wrap-any" style={{ fontSize: 16, fontWeight: 600 }}>
            {name}
          </h2>
          <span className="panel-sub">{o ? `${bytes(o.size)} · ${kind(name)}` : " "}</span>
        </div>
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <a className="btn btn-primary" style={{ flexGrow: 1, fontWeight: 500 }} href={paths.download(bucket, objectKey)} download={name}>
          <DownloadIcon size={15} />
          Download
        </a>
        <button
          type="button"
          className="btn"
          title={uri}
          onClick={async () => {
            const ok = await copyText(uri);
            notify(ok ? `Copied ${uri}` : "Copying is blocked in this browser.", ok ? "ok" : "error");
          }}
        >
          <CopyIcon size={15} />
          Copy S3 URI
        </button>
        <button type="button" className="btn btn-danger btn-icon" aria-label="Delete object" onClick={() => setDeleting(true)}>
          <TrashIcon size={15} />
        </button>
      </div>

      {o && (
        <dl className="kv">
          <div>
            <dt>Key</dt>
            <dd className="mono">{o.key}</dd>
          </div>
          <div>
            <dt>Size</dt>
            <dd className="tnum">{count(o.size)} bytes</dd>
          </div>
          <div>
            <dt>Last modified</dt>
            <dd>{dateTime(o.last_modified)}</dd>
          </div>
          <div>
            <dt>Content-Type</dt>
            <dd className="mono">{o.content_type ?? "binary/octet-stream (none given)"}</dd>
          </div>
          <div>
            <dt>ETag</dt>
            <dd className="mono">&quot;{o.etag}&quot;</dd>
          </div>
          <div>
            <dt>Stored as</dt>
            <dd>
              {o.storage === "inline"
                ? "Inline in the metadata record"
                : `${count(o.blocks)} block${o.blocks === 1 ? "" : "s"} · CRC32C per block`}
              {o.parts > 0 && ` · multipart, ${count(o.parts)} part${o.parts === 1 ? "" : "s"}`}
              {o.encrypted && " · encrypted at rest (SSE-S3)"}
            </dd>
          </div>
          {Object.entries(o.checksums).map(([alg, value]) => (
            <div key={alg}>
              <dt>{alg.toUpperCase()}</dt>
              <dd className="mono">{value}</dd>
            </div>
          ))}
        </dl>
      )}

      {tags.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <span className="section-label">Tags</span>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {tags.map(([k, v]) => (
              <div key={k} className="meta-row">
                <span className="muted">{k}</span>
                <span className="right wrap-any">{v}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {(metadata.length > 0 || headers.length > 0) && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <span className="section-label">Metadata</span>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {metadata.map(([k, v]) => (
              <div key={k} className="meta-row">
                <span className="muted">x-amz-meta-{k}</span>
                <span className="right wrap-any">{v}</span>
              </div>
            ))}
            {headers.map(([k, v]) => (
              <div key={k} className="meta-row">
                <span className="muted">{k}</span>
                <span className="right wrap-any">{v}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="card">
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <LinkIcon size={15} color="var(--muted)" />
          <span style={{ fontWeight: 500 }}>Presigned link</span>
        </div>
        <span style={{ fontSize: 13, color: "var(--subtle)" }}>
          Anyone with the link can download this object from the S3 endpoint until it expires. Signed with SigV4 by your key.
        </span>
        <div style={{ display: "flex", gap: 8, alignItems: "flex-end" }}>
          <label style={{ display: "flex", flexDirection: "column", gap: 6, flexGrow: 1, fontSize: 12, color: "var(--muted)" }}>
            Expires after
            <select className="input" value={expires} onChange={(e) => setExpires(Number(e.target.value))}>
              {EXPIRIES.map((x) => (
                <option key={x.secs} value={x.secs}>
                  {x.label}
                </option>
              ))}
            </select>
          </label>
          <button type="button" className="btn btn-surface" onClick={share} disabled={sharing || !o}>
            {sharing ? "Signing…" : "Copy link"}
          </button>
        </div>
        {link && (
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <div className="codebox" style={{ background: "var(--surface)" }}>
              <code className="ellipsis" title={link.url}>
                {link.url}
              </code>
              <CopyButton text={link.url} label="Copy link" />
            </div>
            <span className="caption">Expires {dateTime(link.expires_at)}</span>
          </div>
        )}
      </div>

      <ConfirmDialog
        open={deleting}
        title={`Delete ${name}?`}
        body={
          <>
            <span className="mono wrap-any">{uri}</span> is removed for every client. Its data is freed after the GC grace period. This cannot be undone.
          </>
        }
        confirm="Delete object"
        onClose={() => setDeleting(false)}
        onConfirm={async () => {
          await api(paths.object(bucket, objectKey), { method: "DELETE" });
          notify(`Deleted ${name}.`);
          onDeleted();
        }}
      />
    </section>
  );
}
