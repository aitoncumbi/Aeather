"use client";

import { useState, type FormEvent } from "react";

import { api, ApiError, type CreatedKey, type Grant } from "@/lib/api";
import { copyText } from "@/lib/hooks";
import { CheckIcon, ChevronRightIcon, CloseIcon, DownloadIcon, PlusIcon, WarningIcon } from "./icons";
import { CopyButton, Dialog, DialogHead, useToast } from "./ui";

type Role = "standard" | "create" | "admin";

const ROLES: { id: Role; label: string; hint: string }[] = [
  { id: "standard", label: "Standard", hint: "Only the buckets granted below" },
  { id: "create", label: "Creates buckets", hint: "Also creates buckets, and owns them" },
  { id: "admin", label: "Admin", hint: "Everything, including the admin API" },
];

const PERMS: { id: keyof Grant; letter: string; name: string }[] = [
  { id: "read", letter: "R", name: "Read" },
  { id: "write", letter: "W", name: "Write" },
  { id: "owner", letter: "O", name: "Owner" },
];

export function CreateKeyDialog({
  open,
  buckets,
  endpoint,
  region,
  onClose,
  onCreated,
}: {
  open: boolean;
  buckets: string[];
  endpoint: string;
  region: string;
  onClose: () => void;
  onCreated: (k: CreatedKey) => void;
}) {
  const [created, setCreated] = useState<CreatedKey | null>(null);
  const close = () => {
    setCreated(null);
    onClose();
  };
  return (
    <Dialog open={open} onClose={close} labelledBy={created ? "ck-done" : "ck-title"}>
      {created ? (
        <Created k={created} endpoint={endpoint} region={region} onDone={close} />
      ) : (
        <KeyForm
          buckets={buckets}
          onCancel={close}
          onCreated={(k) => {
            setCreated(k);
            onCreated(k);
          }}
        />
      )}
    </Dialog>
  );
}

function KeyForm({ buckets, onCancel, onCreated }: { buckets: string[]; onCancel: () => void; onCreated: (k: CreatedKey) => void }) {
  const [name, setName] = useState("");
  const [role, setRole] = useState<Role>("standard");
  const [grants, setGrants] = useState<[string, Grant][]>([]);
  const [adding, setAdding] = useState("");
  const [importId, setImportId] = useState("");
  const [importSecret, setImportSecret] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const unused = buckets.filter((b) => !grants.some(([g]) => g === b));

  function setPerm(i: number, perm: keyof Grant) {
    setGrants(grants.map((g, j) => (j === i ? [g[0], { ...g[1], [perm]: !g[1][perm] }] : g)));
  }

  async function submit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    if ((importId === "") !== (importSecret === "")) {
      setError("To import credentials, give both the access key ID and the secret key.");
      return;
    }
    setBusy(true);
    try {
      const body = {
        name: name.trim(),
        admin: role === "admin",
        can_create_buckets: role === "create",
        buckets: role === "admin" ? {} : Object.fromEntries(grants.filter(([, g]) => g.read || g.write || g.owner)),
        ...(importId ? { access_key_id: importId.trim(), secret_key: importSecret } : {}),
      };
      onCreated(await api<CreatedKey>("/keys", { method: "POST", json: body }));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form onSubmit={submit}>
      <DialogHead id="ck-title" title="Create access key" sub="Give each application its own key, with access to only the buckets it needs." onClose={onCancel} />
      <div className="dialog-body">
        <label className="label">
          Name
          <span className="field field-lg">
            <input autoFocus required placeholder="e.g. billing service" spellCheck={false} value={name} onChange={(e) => setName(e.target.value)} />
          </span>
          <span className="hint">Shown in the key list and the access log. Use the application&apos;s name.</span>
        </label>

        <fieldset style={{ margin: 0, padding: 0, border: 0, display: "flex", flexDirection: "column", gap: 8 }}>
          <legend style={{ padding: "0 0 8px", fontSize: 13, fontWeight: 500, color: "var(--text-2)" }}>Role</legend>
          <div className="roles">
            {ROLES.map((r) => (
              <label key={r.id} className="role">
                <span className="role-name">
                  <input type="radio" name="role" checked={role === r.id} onChange={() => setRole(r.id)} style={{ width: 16, height: 16, margin: 0 }} />
                  {r.label}
                </span>
                <span className="hint">{r.hint}</span>
              </label>
            ))}
          </div>
        </fieldset>

        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between" }}>
            <span style={{ fontSize: 13, fontWeight: 500, color: "var(--text-2)" }}>Bucket access</span>
            <span className="hint">R read · W write · O owner</span>
          </div>
          {role === "admin" ? (
            <div style={{ padding: 14, fontSize: 13, color: "var(--muted)", background: "var(--chip)" }}>
              Admin keys reach every bucket and the admin API. Prefer a standard key unless this is for an operator.
            </div>
          ) : (
            <div className="ruled-list">
              {grants.map(([bucket, g], i) => (
                <div key={bucket} style={{ display: "flex", alignItems: "center", gap: 10, minHeight: 52 }}>
                  <span className="mono ellipsis" style={{ flexGrow: 1, fontSize: 13 }}>
                    {bucket}
                  </span>
                  <div role="group" aria-label={`Permissions on ${bucket}`} style={{ display: "flex", gap: 4 }}>
                    {PERMS.map((p) => (
                      <button key={p.id} type="button" className="perm" aria-pressed={g[p.id]} aria-label={`${p.name} on ${bucket}`} onClick={() => setPerm(i, p.id)}>
                        {p.letter}
                      </button>
                    ))}
                  </div>
                  <button
                    type="button"
                    className="btn btn-quiet btn-icon"
                    style={{ width: 32, height: 32 }}
                    aria-label={`Remove ${bucket}`}
                    onClick={() => setGrants(grants.filter((_, j) => j !== i))}
                  >
                    <CloseIcon size={14} />
                  </button>
                </div>
              ))}
              {unused.length > 0 ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8, minHeight: 52 }}>
                  <label className="sr-only" htmlFor="ck-add">
                    Bucket to add
                  </label>
                  <select id="ck-add" className="input" style={{ flexGrow: 1, fontFamily: "var(--mono)", fontSize: 13 }} value={adding} onChange={(e) => setAdding(e.target.value)}>
                    <option value="">Choose a bucket…</option>
                    {unused.map((b) => (
                      <option key={b} value={b}>
                        {b}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    className="btn btn-sm"
                    disabled={!adding}
                    onClick={() => {
                      setGrants([...grants, [adding, { read: true, write: false, owner: false }]]);
                      setAdding("");
                    }}
                  >
                    <PlusIcon size={14} />
                    Add
                  </button>
                </div>
              ) : (
                <span className="hint" style={{ padding: "12px 0" }}>
                  {buckets.length === 0 ? "No buckets exist yet; grant access later on the key's page." : "Every bucket is listed."}
                </span>
              )}
            </div>
          )}
        </div>

        <details className="disclosure">
          <summary>
            <ChevronRightIcon size={14} />
            Import existing credentials (migrating from another S3 store)
          </summary>
          <div style={{ display: "flex", flexDirection: "column", gap: 12, padding: "4px 0 14px" }}>
            <label style={{ display: "flex", flexDirection: "column", gap: 6, fontSize: 12, color: "var(--muted)" }}>
              Access key ID
              <input className="input mono" spellCheck={false} placeholder="AKIA…" value={importId} onChange={(e) => setImportId(e.target.value)} style={{ fontSize: 13 }} />
            </label>
            <label style={{ display: "flex", flexDirection: "column", gap: 6, fontSize: 12, color: "var(--muted)" }}>
              Secret key
              <input
                className="input mono"
                type="password"
                autoComplete="new-password"
                spellCheck={false}
                placeholder="Paste the existing secret"
                value={importSecret}
                onChange={(e) => setImportSecret(e.target.value)}
                style={{ fontSize: 13 }}
              />
            </label>
          </div>
        </details>

        {error && (
          <p className="error-text" role="alert">
            {error}
          </p>
        )}
      </div>
      <div className="dialog-foot">
        <button type="button" className="btn" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="btn btn-primary" disabled={busy || !name.trim()}>
          {busy ? "Creating…" : "Create key"}
        </button>
      </div>
    </form>
  );
}

function Created({ k, endpoint, region, onDone }: { k: CreatedKey; endpoint: string; region: string; onDone: () => void }) {
  const [saved, setSaved] = useState(false);
  const notify = useToast();
  const granted = Object.keys(k.buckets);
  const summary = k.admin
    ? "The new admin key can reach every bucket and the admin API."
    : `It can access ${granted.length ? granted.join(", ") : "no buckets yet"}${k.can_create_buckets ? ", and create buckets of its own" : ""}.`;
  const env = [
    `AWS_ACCESS_KEY_ID=${k.access_key_id}`,
    `AWS_SECRET_ACCESS_KEY=${k.secret_key}`,
    `AWS_ENDPOINT_URL=${endpoint}`,
    `AWS_REGION=${region}`,
  ].join("\n");

  function downloadEnv() {
    const url = URL.createObjectURL(new Blob([`${env}\n`], { type: "text/plain" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${k.name.replace(/[^a-zA-Z0-9._-]+/g, "-") || "aether"}.env`;
    a.click();
    URL.revokeObjectURL(url);
  }

  return (
    <>
      <div className="dialog-body" style={{ gap: 18 }}>
        <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
          <div style={{ width: 40, height: 40, background: "var(--ok-bg)", color: "var(--ok-text)", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
            <CheckIcon size={18} />
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <h2 id="ck-done" className="dialog-title">
              Key created
            </h2>
            <span className="muted" style={{ fontSize: 13 }}>
              {summary}
            </span>
          </div>
        </div>

        <div className="notice" style={{ padding: "12px 14px" }}>
          <WarningIcon size={18} color="var(--warn)" />
          <span>
            <span style={{ fontWeight: 600 }}>Save the secret key now.</span>{" "}
            <span className="muted">It cannot be shown again. If it is lost, create a new key and delete this one.</span>
          </span>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          <Secret label="Access key ID" value={k.access_key_id} />
          <Secret label="Secret key" value={k.secret_key} />
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span style={{ fontSize: 12, color: "var(--muted)" }}>For the application&apos;s environment</span>
            <button type="button" className="btn btn-xs" onClick={async () => notify((await copyText(env)) ? "Copied." : "Copying is blocked in this browser.")}>
              Copy all
            </button>
          </div>
          <pre className="env">{env}</pre>
        </div>

        <label style={{ display: "flex", alignItems: "center", gap: 10, minHeight: 36, fontSize: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={saved} onChange={(e) => setSaved(e.target.checked)} style={{ width: 18, height: 18, margin: 0 }} />I have saved the secret key somewhere safe
        </label>
      </div>
      <div className="dialog-foot">
        <button type="button" className="btn" onClick={downloadEnv}>
          <DownloadIcon size={15} />
          Download .env
        </button>
        <button type="button" className="btn btn-primary" disabled={!saved} onClick={onDone}>
          Done
        </button>
      </div>
    </>
  );
}

function Secret({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <span style={{ fontSize: 12, color: "var(--muted)" }}>{label}</span>
      <div className="codebox" style={{ background: "var(--field)", padding: "0 6px 0 12px" }}>
        <code className="wrap-any" style={{ fontSize: 13, color: "var(--text)" }}>
          {value}
        </code>
        <CopyButton text={value} label={`Copy ${label.toLowerCase()}`} />
      </div>
    </div>
  );
}
