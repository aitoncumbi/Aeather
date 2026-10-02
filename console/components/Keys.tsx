"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useState } from "react";

import { api, paths, type Bucket, type Grant, type Key } from "@/lib/api";
import { date } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { CreateKeyDialog } from "./CreateKeyDialog";
import { KeyIcon, PlusIcon, SearchIcon, ShieldIcon } from "./icons";
import { useShell } from "./Shell";
import { ConfirmDialog, CopyButton, ErrorPanel, Switch, useToast } from "./ui";

type Filter = "all" | "active" | "disabled" | "admin";

const FILTERS: { id: Filter; label: string; match: (k: Key) => boolean }[] = [
  { id: "all", label: "All", match: () => true },
  { id: "active", label: "Active", match: (k) => !k.disabled },
  { id: "disabled", label: "Disabled", match: (k) => k.disabled },
  { id: "admin", label: "Admin", match: (k) => k.admin },
];

export function roleOf(k: Pick<Key, "admin" | "can_create_buckets">): string {
  return k.admin ? "Admin" : k.can_create_buckets ? "Creates buckets" : "Standard";
}

function letters(g: Grant): string {
  return `${g.read ? "R" : ""}${g.write ? "W" : ""}${g.owner ? "O" : ""}`;
}

export function Keys() {
  const router = useRouter();
  const params = useSearchParams();
  const { session, status, refreshStatus } = useShell();
  const keys = useApi<Key[]>("/keys", 30_000);
  const buckets = useApi<Bucket[]>("/buckets", 60_000);
  const [filter, setFilter] = useState<Filter>("all");
  const [search, setSearch] = useState("");
  const [creating, setCreating] = useState(false);

  const all = keys.data ?? [];
  const q = search.trim().toLowerCase();
  const match = FILTERS.find((f) => f.id === filter)!.match;
  const rows = all.filter((k) => match(k) && (!q || k.name.toLowerCase().includes(q) || k.access_key_id.toLowerCase().includes(q)));
  const selectedId = params.get("id") ?? rows[0]?.access_key_id ?? null;
  const selected = all.find((k) => k.access_key_id === selectedId) ?? null;
  const select = (id: string) => router.replace(`/keys/?id=${encodeURIComponent(id)}`, { scroll: false });

  function replaceKey(updated: Key) {
    if (keys.data) keys.replace(keys.data.map((k) => (k.access_key_id === updated.access_key_id ? updated : k)));
  }

  return (
    <div className="page" style={{ gap: 18 }}>
      <div className="head">
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <h1 className="title">Access keys</h1>
          <span className="subtitle">One key per application. Secrets are shown once, when a key is created.</span>
        </div>
        <button type="button" className="btn btn-primary" style={{ fontWeight: 500 }} onClick={() => setCreating(true)}>
          <PlusIcon />
          Create key
        </button>
      </div>

      <div className="banner">
        <div className="logo" style={{ width: 36, height: 36 }}>
          <ShieldIcon />
        </div>
        <div className="row-text">
          <span style={{ fontWeight: 500 }}>
            Root key <span className="mono muted" style={{ fontWeight: 400 }}>{session.root_access_key_id}</span>
          </span>
          <span className="row-hint">
            Set in the configuration file and can do everything. Change it there; it cannot be edited or disabled from the console. Give
            applications their own keys instead.
          </span>
        </div>
        {session.root && (
          <span className="chip hide-sm" style={{ color: "var(--muted)", padding: "4px 10px" }}>
            You are signed in with it
          </span>
        )}
      </div>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap", padding: "0 24px" }}>
        <div className="segmented" role="group" aria-label="Filter keys">
          {FILTERS.map((f) => (
            <button key={f.id} type="button" aria-pressed={filter === f.id} onClick={() => setFilter(f.id)}>
              {f.label}
              <span className="count">{all.filter(f.match).length}</span>
            </button>
          ))}
        </div>
        <label className="field" style={{ width: 280, maxWidth: "100%" }}>
          <SearchIcon size={15} />
          <span className="sr-only">Search keys</span>
          <input type="search" placeholder="Search by name or key ID" value={search} onChange={(e) => setSearch(e.target.value)} />
        </label>
      </div>

      {keys.error ? (
        <ErrorPanel message={keys.error.message} onRetry={keys.reload} />
      ) : (
        <div className="ruled split-keys">
          <section aria-label="Key list" className="list">
            <div className="thead cols-keys">
              <span>Name</span>
              <span className="hide-sm">Role</span>
              <span className="hide-sm">Bucket access</span>
              <span>Status</span>
            </div>
            {rows.map((k) => {
              const granted = Object.entries(k.buckets).sort(([a], [b]) => a.localeCompare(b));
              const chips = k.admin
                ? ["all buckets"]
                : granted.length === 0
                  ? ["none"]
                  : granted.slice(0, 2).map(([b, g]) => `${b} ${letters(g)}`).concat(granted.length > 2 ? [`+${granted.length - 2}`] : []);
              return (
                <button
                  key={k.access_key_id}
                  type="button"
                  className="trow cols-keys"
                  style={{ minHeight: 60, padding: "8px 24px" }}
                  aria-pressed={k.access_key_id === selectedId}
                  onClick={() => select(k.access_key_id)}
                >
                  <span style={{ display: "flex", flexDirection: "column", gap: 2, minWidth: 0 }}>
                    <span className="ellipsis" style={{ fontWeight: 500, color: k.disabled ? "var(--muted)" : "var(--text)" }}>
                      {k.name}
                    </span>
                    <span className="mono subtle ellipsis" style={{ fontSize: 12 }}>
                      {k.access_key_id}
                    </span>
                  </span>
                  <span className="hide-sm">
                    <span className="chip" style={{ background: k.admin ? "#1f2937" : "var(--chip)" }}>
                      {roleOf(k)}
                    </span>
                  </span>
                  <span className="hide-sm" style={{ display: "flex", flexWrap: "wrap", gap: 4, minWidth: 0 }}>
                    {chips.map((c) => (
                      <span key={c} className="chip-mono">
                        {c}
                      </span>
                    ))}
                  </span>
                  <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, color: k.disabled ? "var(--muted)" : "var(--text-2)" }}>
                    <span className="dot" style={{ width: 7, height: 7, background: k.disabled ? "#52525b" : "var(--ok)" }} />
                    {k.disabled ? "Disabled" : "Active"}
                  </span>
                </button>
              );
            })}
            {keys.data && rows.length === 0 && (
              <div className="empty">
                <div className="empty-icon">
                  <KeyIcon size={18} />
                </div>
                {all.length === 0 ? (
                  <>
                    <span style={{ fontWeight: 500, color: "var(--text)" }}>No access keys yet</span>
                    <span style={{ fontSize: 13 }}>Create one for each application that uses Aether.</span>
                  </>
                ) : (
                  <span>No key matches.</span>
                )}
              </div>
            )}
          </section>
          {selected ? (
            <KeyDetail
              key={selected.access_key_id}
              k={selected}
              buckets={(buckets.data ?? []).map((b) => b.name)}
              signedInWithIt={selected.access_key_id === session.access_key_id}
              onChange={replaceKey}
              onDeleted={() => {
                keys.reload();
                refreshStatus();
                router.replace("/keys/", { scroll: false });
              }}
            />
          ) : (
            <section className="panel">
              <span className="caption">{keys.data ? "Select a key to see its permissions." : "Loading…"}</span>
            </section>
          )}
        </div>
      )}

      <CreateKeyDialog
        open={creating}
        buckets={(buckets.data ?? []).map((b) => b.name)}
        endpoint={status?.s3_endpoint ?? ""}
        region={status?.region ?? "us-east-1"}
        onClose={() => setCreating(false)}
        onCreated={(k) => {
          keys.reload();
          refreshStatus();
          select(k.access_key_id);
        }}
      />
    </div>
  );
}

function KeyDetail({
  k,
  buckets,
  signedInWithIt,
  onChange,
  onDeleted,
}: {
  k: Key;
  buckets: string[];
  signedInWithIt: boolean;
  onChange: (k: Key) => void;
  onDeleted: () => void;
}) {
  const notify = useToast();
  const [deleting, setDeleting] = useState(false);
  const [busy, setBusy] = useState(false);

  async function patch(change: Partial<Pick<Key, "disabled" | "admin" | "can_create_buckets">>, message: string) {
    setBusy(true);
    try {
      onChange(await api<Key>(paths.key(k.access_key_id), { method: "PATCH", json: change }));
      notify(message);
    } catch (e) {
      notify(e instanceof Error ? e.message : String(e), "error");
    } finally {
      setBusy(false);
    }
  }

  async function toggle(bucket: string, perm: keyof Grant) {
    const current = k.buckets[bucket] ?? { read: false, write: false, owner: false };
    const next = { ...current, [perm]: !current[perm] };
    const none = !next.read && !next.write && !next.owner;
    const buckets = { ...k.buckets };
    if (none) delete buckets[bucket];
    else buckets[bucket] = next;
    // Show the change at once; put the old state back if the server refuses.
    onChange({ ...k, buckets });
    try {
      if (none) await api(paths.grant(k.access_key_id, bucket), { method: "DELETE" });
      else onChange(await api<Key>(paths.grant(k.access_key_id, bucket), { method: "PUT", json: next }));
    } catch (e) {
      onChange(k);
      notify(e instanceof Error ? e.message : String(e), "error");
    }
  }

  // Buckets that no longer exist can still hold grants; show those too.
  const rows = [...new Set([...buckets, ...Object.keys(k.buckets)])].sort();
  const lockHint = "You are signed in with this key; use another admin key or the root key to change this.";

  return (
    <section aria-labelledby="key-title" className="panel" style={{ gap: 18 }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
        <h2 id="key-title" style={{ fontSize: 18, fontWeight: 600, letterSpacing: "-0.01em" }}>
          {k.name}
        </h2>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span className="mono muted" style={{ fontSize: 13 }}>
            {k.access_key_id}
          </span>
          <CopyButton text={k.access_key_id} label="Copy access key ID" size="xs" />
        </div>
        <span className="panel-sub">Created {date(k.created_at)}</span>
      </div>

      <div className="ruled-list">
        <SwitchRow
          label="Enabled"
          hint={signedInWithIt ? lockHint : k.disabled ? "Requests with this key are refused" : "Takes effect immediately when turned off"}
          checked={!k.disabled}
          disabled={busy || signedInWithIt}
          onChange={(on) => patch({ disabled: !on }, on ? `Enabled ${k.name}.` : `Disabled ${k.name}.`)}
        />
        <SwitchRow
          label="Admin"
          hint={signedInWithIt ? lockHint : "Full access to every bucket, the admin API and this console"}
          checked={k.admin}
          disabled={busy || signedInWithIt}
          onChange={(on) => patch({ admin: on }, on ? `${k.name} is now an admin key.` : `${k.name} is no longer an admin key.`)}
        />
        <SwitchRow
          label="Can create buckets"
          hint="Owns the buckets it creates"
          checked={k.can_create_buckets || k.admin}
          disabled={busy || k.admin}
          onChange={(on) => patch({ can_create_buckets: on }, on ? `${k.name} can create buckets.` : `${k.name} can no longer create buckets.`)}
        />
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12 }}>
          <span className="section-label">Bucket permissions</span>
          <span className="caption">Changes apply immediately</span>
        </div>
        {k.admin ? (
          <div style={{ padding: 14, fontSize: 13, color: "var(--muted)", background: "var(--chip)" }}>
            Admin keys can read, write and manage every bucket, so per-bucket permissions do not apply.
          </div>
        ) : rows.length === 0 ? (
          <div style={{ padding: 14, fontSize: 13, color: "var(--muted)", background: "var(--chip)" }}>No buckets exist yet.</div>
        ) : (
          <>
            <table className="data" aria-label="Permissions per bucket" style={{ borderBottom: "1px solid var(--rule)" }}>
              <thead>
                <tr>
                  <th scope="col" style={{ paddingLeft: 0 }}>
                    Bucket
                  </th>
                  <th scope="col" style={{ textAlign: "center", width: 56 }}>
                    Read
                  </th>
                  <th scope="col" style={{ textAlign: "center", width: 56 }}>
                    Write
                  </th>
                  <th scope="col" style={{ textAlign: "center", width: 56 }}>
                    Owner
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.map((bucket) => {
                  const g = k.buckets[bucket];
                  return (
                    <tr key={bucket} style={{ height: 44 }}>
                      <td className="mono ellipsis" style={{ paddingLeft: 0, color: g ? "var(--text)" : "var(--muted)", borderTop: "1px solid var(--rule)", maxWidth: 0 }}>
                        {bucket}
                        {!buckets.includes(bucket) && <span className="subtle"> (deleted)</span>}
                      </td>
                      {(["read", "write", "owner"] as const).map((perm) => (
                        <td key={perm} style={{ textAlign: "center", borderTop: "1px solid var(--rule)" }}>
                          <input
                            type="checkbox"
                            checked={!!g?.[perm]}
                            onChange={() => toggle(bucket, perm)}
                            aria-label={`${perm[0]!.toUpperCase()}${perm.slice(1)} on ${bucket}`}
                            style={{ width: 18, height: 18, margin: 0, cursor: "pointer" }}
                          />
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
            <span className="caption">
              Read: get and list. Write: put, copy, delete, multipart. Owner: delete the bucket and change its settings.
            </span>
          </>
        )}
      </div>

      <div className="danger-zone">
        <div className="row-text">
          <span className="row-title">Delete key</span>
          <span className="row-hint" style={{ color: "var(--muted)" }}>
            {signedInWithIt ? lockHint : "Applications using it lose access at once. Disabling is reversible."}
          </span>
        </div>
        <button type="button" className="btn btn-danger btn-sm" style={{ fontWeight: 500 }} disabled={signedInWithIt} onClick={() => setDeleting(true)}>
          Delete
        </button>
      </div>

      <ConfirmDialog
        open={deleting}
        title={`Delete ${k.name}?`}
        body={
          <>
            Applications using <span className="mono">{k.access_key_id}</span> are refused from now on. The key cannot be restored; disable it instead
            if you may need it again.
          </>
        }
        confirm="Delete key"
        onClose={() => setDeleting(false)}
        onConfirm={async () => {
          await api(paths.key(k.access_key_id), { method: "DELETE" });
          notify(`Deleted ${k.name}.`);
          onDeleted();
        }}
      />
    </section>
  );
}

function SwitchRow({
  label,
  hint,
  checked,
  disabled,
  onChange,
}: {
  label: string;
  hint: string;
  checked: boolean;
  disabled?: boolean;
  onChange: (on: boolean) => void;
}) {
  return (
    <div className="row">
      <div className="row-text">
        <span className="row-title">{label}</span>
        <span className="row-hint">{hint}</span>
      </div>
      <Switch label={label} checked={checked} disabled={disabled} onChange={onChange} />
    </div>
  );
}
