"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useState, type FormEvent } from "react";

import { api, ApiError, paths, type Bucket, type CorsRule } from "@/lib/api";
import { bytes, count, date, percent } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { BucketIcon, ChevronRightIcon, PlusIcon, SearchIcon, TrashIcon } from "./icons";
import { useShell } from "./Shell";
import { ConfirmDialog, CopyButton, Dialog, DialogHead, ErrorPanel, useToast } from "./ui";

const NAME_RULE = /^(?=.{3,63}$)[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

export function Buckets() {
  const router = useRouter();
  const params = useSearchParams();
  const { status, refreshStatus } = useShell();
  const list = useApi<Bucket[]>("/buckets", 30_000);
  const [filter, setFilter] = useState("");
  const [creating, setCreating] = useState(false);

  const buckets = list.data ?? [];
  const selectedName = params.get("name") ?? buckets[0]?.name ?? null;
  const select = (name: string) => router.replace(`/buckets/?name=${encodeURIComponent(name)}`, { scroll: false });
  const shown = buckets.filter((b) => b.name.includes(filter.trim().toLowerCase()));
  const largest = Math.max(1, ...buckets.map((b) => b.bytes));
  const totalBytes = buckets.reduce((a, b) => a + b.bytes, 0);
  const totalObjects = buckets.reduce((a, b) => a + b.objects, 0);

  return (
    <div className="page">
      <div className="head">
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <h1 className="title">Buckets</h1>
          <span className="subtitle">
            {list.data
              ? `${count(buckets.length)} bucket${buckets.length === 1 ? "" : "s"} · ${bytes(totalBytes)} · ${count(totalObjects)} objects`
              : "Loading…"}
          </span>
        </div>
        <div className="head-actions">
          <label className="field" style={{ minWidth: 240, flexGrow: 1 }}>
            <SearchIcon size={15} />
            <span className="sr-only">Filter buckets</span>
            <input type="search" placeholder="Filter by name" value={filter} onChange={(e) => setFilter(e.target.value)} />
          </label>
          <button type="button" className="btn btn-primary" style={{ fontWeight: 500 }} onClick={() => setCreating(true)}>
            <PlusIcon />
            Create bucket
          </button>
        </div>
      </div>

      {list.error ? (
        <ErrorPanel message={list.error.message} onRetry={list.reload} />
      ) : (
        <div className="ruled split">
          <section aria-label="Bucket list" className="list">
            <div className="thead cols-buckets">
              <span>Name</span>
              <span className="right hide-sm">Objects</span>
              <span>Size</span>
              <span className="right hide-sm">Keys</span>
              <span className="hide-sm">Created</span>
            </div>
            {shown.map((b) => (
              <button
                key={b.name}
                type="button"
                className="trow cols-buckets"
                aria-pressed={b.name === selectedName}
                onClick={() => select(b.name)}
              >
                <span style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                  <span className="cell-icon">
                    <BucketIcon size={14} />
                  </span>
                  <span className="mono ellipsis" style={{ fontSize: 13 }}>
                    {b.name}
                  </span>
                </span>
                <span className="right tnum muted hide-sm">{count(b.objects)}</span>
                <span style={{ display: "flex", alignItems: "center", gap: 10, minWidth: 0 }}>
                  <span className="tnum right" style={{ width: 64, flexShrink: 0 }}>
                    {bytes(b.bytes)}
                  </span>
                  <span className="hbar hide-sm" style={{ flexGrow: 1, height: 6 }}>
                    <div style={{ width: `${((b.bytes / largest) * 100).toFixed(1)}%` }} />
                  </span>
                </span>
                <span className="right tnum muted hide-sm">{b.key_count}</span>
                <span className="muted hide-sm" style={{ fontSize: 13 }}>
                  {date(b.created_at)}
                </span>
              </button>
            ))}
            {list.data && shown.length === 0 && (
              <div className="empty">
                {buckets.length === 0 ? (
                  <>
                    <div className="empty-icon">
                      <BucketIcon size={18} />
                    </div>
                    <span style={{ fontWeight: 500, color: "var(--text)" }}>No buckets yet</span>
                    <span style={{ fontSize: 13 }}>Create one here, or with any S3 client.</span>
                  </>
                ) : (
                  "No bucket matches that name."
                )}
              </div>
            )}
          </section>
          {selectedName ? (
            <BucketDetail
              key={selectedName}
              name={selectedName}
              totalBytes={status?.bytes ?? totalBytes}
              endpoint={status?.s3_endpoint ?? ""}
              onDeleted={() => {
                list.reload();
                refreshStatus();
                router.replace("/buckets/", { scroll: false });
              }}
            />
          ) : (
            <section className="panel">
              <span className="caption">Select a bucket to see its details.</span>
            </section>
          )}
        </div>
      )}

      <CreateBucketDialog
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(name) => {
          list.reload();
          refreshStatus();
          select(name);
        }}
      />
    </div>
  );
}

function BucketDetail({
  name,
  totalBytes,
  endpoint,
  onDeleted,
}: {
  name: string;
  totalBytes: number;
  endpoint: string;
  onDeleted: () => void;
}) {
  const detail = useApi<Bucket>(paths.bucket(name), 30_000);
  const [deleting, setDeleting] = useState(false);
  const notify = useToast();
  if (detail.error) {
    return (
      <section className="panel">
        <ErrorPanel message={detail.error.status === 404 ? `There is no bucket named ${name}.` : detail.error.message} />
      </section>
    );
  }
  const b = detail.data;
  const command = `aws --endpoint-url ${endpoint} s3 ls s3://${name}`;
  return (
    <section aria-labelledby="bk-title" className="panel" style={{ gap: 20 }}>
      <div className="panel-head">
        <div style={{ display: "flex", flexDirection: "column", gap: 4, minWidth: 0 }}>
          <h2 id="bk-title" className="mono wrap-any" style={{ fontSize: 18, fontWeight: 600, letterSpacing: "-0.01em" }}>
            {name}
          </h2>
          <span className="panel-sub">{b ? `Created ${date(b.created_at)}` : " "}</span>
        </div>
        <Link href={`/objects/?bucket=${encodeURIComponent(name)}`} className="btn btn-sm" style={{ fontWeight: 500 }}>
          Browse
          <ChevronRightIcon size={14} />
        </Link>
      </div>

      <dl className="facts">
        <div>
          <dt>Objects</dt>
          <dd className="tnum">{b ? count(b.objects) : "—"}</dd>
        </div>
        <div>
          <dt>Size</dt>
          <dd className="tnum">{b ? bytes(b.bytes) : "—"}</dd>
        </div>
        <div>
          <dt>Of all data</dt>
          <dd className="tnum">{b ? percent(b.bytes, totalBytes) : "—"}</dd>
        </div>
      </dl>

      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <span className="section-label">Connect</span>
        <div className="codebox">
          <code className="ellipsis">{command}</code>
          <CopyButton text={command} label="Copy command" />
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <span className="section-label">Keys with access</span>
          <Link href="/keys/" style={{ fontSize: 13, minHeight: 32, display: "flex", alignItems: "center", gap: 4 }}>
            <PlusIcon size={14} />
            Grant a key
          </Link>
        </div>
        <div className="ruled-list">
          {b?.keys?.map((k) => (
            <div key={k.access_key_id} className="row">
              <Link href={`/keys/?id=${encodeURIComponent(k.access_key_id)}`} className="row-text" style={{ color: "var(--text)" }}>
                <span className="row-title">{k.name}</span>
                <span className="mono subtle" style={{ fontSize: 12 }}>
                  {k.access_key_id}
                </span>
              </Link>
              <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                {k.read && <span className="chip">Read</span>}
                {k.write && <span className="chip">Write</span>}
                {k.owner && <span className="chip">Owner</span>}
              </div>
            </div>
          ))}
          {b?.keys && b.keys.length === 0 && (
            <div className="row">
              <span className="row-hint">No application key has access yet.</span>
            </div>
          )}
        </div>
        <span className="caption">Admin keys and the root key can reach every bucket and are not listed.</span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        <span className="section-label">Settings</span>
        <div className="ruled-list">
          <QuotaSetting bucket={b} onChanged={detail.reload} />
          <CorsSetting rules={b?.cors} />
          <VersioningSetting bucket={b} onChanged={detail.reload} />
        </div>
      </div>

      <div className="danger-zone">
        <div className="row-text">
          <span className="row-title">Delete bucket</span>
          <span className="row-hint" style={{ color: "var(--muted)" }}>
            {!b
              ? " "
              : b.objects > 0
                ? `Only empty buckets can be deleted. This one holds ${count(b.objects)} object${b.objects === 1 ? "" : "s"}.`
                : "The bucket is empty and can be deleted."}
          </span>
        </div>
        <button type="button" className="btn btn-danger btn-sm" disabled={!b || b.objects > 0} onClick={() => setDeleting(true)} style={{ fontWeight: 500 }}>
          <TrashIcon size={14} />
          Delete
        </button>
      </div>

      <ConfirmDialog
        open={deleting}
        title={`Delete ${name}?`}
        body="The bucket is empty. Its permissions on every key are removed too. This cannot be undone."
        confirm="Delete bucket"
        onClose={() => setDeleting(false)}
        onConfirm={async () => {
          await api(paths.bucket(name), { method: "DELETE" });
          notify(`Deleted bucket ${name}.`);
          onDeleted();
        }}
      />
    </section>
  );
}

function QuotaSetting({ bucket, onChanged }: { bucket: Bucket | undefined; onChanged: () => void }) {
  const [editing, setEditing] = useState(false);
  const q = bucket?.quota;
  const limits = [
    q?.max_bytes != null && `${bytes(bucket!.bytes)} of ${bytes(q.max_bytes)}`,
    q?.max_objects != null && `${count(bucket!.objects)} of ${count(q.max_objects)} objects`,
  ].filter(Boolean);
  return (
    <div className="row">
      <div className="row-text">
        <span className="row-title">Quota</span>
        <span className="row-hint">{!bucket ? " " : limits.length ? limits.join(" · ") : "No limit"}</span>
      </div>
      <button type="button" className="btn btn-sm" disabled={!bucket} onClick={() => setEditing(true)}>
        Edit
      </button>
      {bucket && (
        <QuotaDialog
          key={String(editing)}
          open={editing}
          bucket={bucket}
          onClose={() => setEditing(false)}
          onSaved={onChanged}
        />
      )}
    </div>
  );
}

const GB = 1e9;

function QuotaDialog({ open, bucket, onClose, onSaved }: { open: boolean; bucket: Bucket; onClose: () => void; onSaved: () => void }) {
  const [size, setSize] = useState(bucket.quota.max_bytes == null ? "" : String(bucket.quota.max_bytes / GB));
  const [objects, setObjects] = useState(bucket.quota.max_objects == null ? "" : String(bucket.quota.max_objects));
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const notify = useToast();
  const sizeOk = size.trim() === "" || Number(size) > 0;
  const objectsOk = objects.trim() === "" || (Number.isInteger(Number(objects)) && Number(objects) > 0);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const quota = {
        max_bytes: size.trim() === "" ? null : Math.round(Number(size) * GB),
        max_objects: objects.trim() === "" ? null : Number(objects),
      };
      await api(`${paths.bucket(bucket.name)}/quota`, { method: "PUT", json: quota });
      notify(`Updated the quota of ${bucket.name}.`);
      onSaved();
      onClose();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Dialog open={open} onClose={onClose} labelledBy="quota-title">
      <form onSubmit={submit}>
        <DialogHead
          id="quota-title"
          title="Bucket quota"
          sub="Uploads that would go past a limit are refused. Leave a field empty for no limit."
          onClose={onClose}
        />
        <div className="dialog-body">
          <label className="label">
            Largest total size, in GB
            <span className="field field-lg">
              <input inputMode="decimal" value={size} onChange={(e) => setSize(e.target.value)} placeholder="No limit" aria-invalid={!sizeOk} />
            </span>
            <span className="hint">Now {bytes(bucket.bytes)}.</span>
          </label>
          <label className="label">
            Most objects
            <span className="field field-lg">
              <input inputMode="numeric" value={objects} onChange={(e) => setObjects(e.target.value)} placeholder="No limit" aria-invalid={!objectsOk} />
            </span>
            <span className="hint">Now {count(bucket.objects)}.</span>
          </label>
          {error && (
            <p className="error-text" role="alert">
              {error}
            </p>
          )}
        </div>
        <div className="dialog-foot">
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={!sizeOk || !objectsOk || busy}>
            {busy ? "Saving…" : "Save quota"}
          </button>
        </div>
      </form>
    </Dialog>
  );
}

function VersioningSetting({ bucket, onChanged }: { bucket: Bucket | undefined; onChanged: () => void }) {
  const notify = useToast();
  const [busy, setBusy] = useState(false);
  const on = bucket?.versioning === "Enabled";
  async function toggle() {
    if (!bucket) return;
    setBusy(true);
    try {
      await api(`${paths.bucket(bucket.name)}/versioning`, { method: "PUT", json: { status: on ? "Suspended" : "Enabled" } });
      notify(on ? `Suspended versioning on ${bucket.name}.` : `Enabled versioning on ${bucket.name}.`);
      onChanged();
    } catch (err) {
      notify(err instanceof ApiError ? err.message : String(err), "error");
    } finally {
      setBusy(false);
    }
  }
  const older = bucket && bucket.noncurrent_objects > 0 ? ` · ${count(bucket.noncurrent_objects)} older versions (${bytes(bucket.noncurrent_bytes)})` : "";
  const hint = !bucket
    ? " "
    : bucket.versioning === "Enabled"
      ? `On: overwrites and deletes keep the previous version${older}`
      : bucket.versioning === "Suspended"
        ? `Suspended: writes replace the current version; older versions are kept${older}`
        : "Off: overwrites and deletes replace data for good";
  return (
    <div className="row">
      <div className="row-text">
        <span className="row-title">Versioning</span>
        <span className="row-hint">{hint}</span>
      </div>
      <button type="button" className="btn btn-sm" disabled={!bucket || busy} onClick={toggle}>
        {on ? "Suspend" : "Enable"}
      </button>
    </div>
  );
}

/** Read-only: rules are set with the S3 API (PutBucketCors). */
function CorsSetting({ rules }: { rules: CorsRule[] | undefined }) {
  const origins = [...new Set(rules?.flatMap((r) => r.allowed_origins) ?? [])];
  return (
    <div className="row">
      <div className="row-text">
        <span className="row-title">CORS</span>
        <span className="row-hint">
          {!rules ? (
            " "
          ) : origins.length === 0 ? (
            <>
              No rules: browsers cannot call this bucket. Set rules with <code>aws s3api put-bucket-cors</code>.
            </>
          ) : (
            `Browsers at ${origins.join(", ")}`
          )}
        </span>
      </div>
      {rules && rules.length > 0 && (
        <span className="chip">
          {rules.length} rule{rules.length === 1 ? "" : "s"}
        </span>
      )}
    </div>
  );
}

function CreateBucketDialog({ open, onClose, onCreated }: { open: boolean; onClose: () => void; onCreated: (name: string) => void }) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const notify = useToast();
  const valid = NAME_RULE.test(name);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api("/buckets", { method: "POST", json: { name } });
      notify(`Created bucket ${name}.`);
      onCreated(name);
      setName("");
      onClose();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Dialog open={open} onClose={onClose} labelledBy="cb-title">
      <form onSubmit={submit}>
        <DialogHead id="cb-title" title="Create bucket" sub="Buckets created here have no owner key; grant keys access on the Access keys page." onClose={onClose} />
        <div className="dialog-body">
          <label className="label">
            Name
            <span className="field field-lg">
              <input
                autoFocus
                spellCheck={false}
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. invoices"
                style={{ fontFamily: "var(--mono)" }}
                aria-describedby="cb-rule"
              />
            </span>
            <span id="cb-rule" className="hint">
              3 to 63 characters: lowercase letters, digits, hyphens and dots, starting and ending with a letter or digit.
            </span>
          </label>
          {error && (
            <p className="error-text" role="alert">
              {error}
            </p>
          )}
        </div>
        <div className="dialog-foot">
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={!valid || busy}>
            {busy ? "Creating…" : "Create bucket"}
          </button>
        </div>
      </form>
    </Dialog>
  );
}
