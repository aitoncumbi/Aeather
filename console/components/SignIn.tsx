"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useState, type FormEvent } from "react";

import { api, ApiError, type Health, type Session } from "@/lib/api";
import { EyeIcon, EyeOffIcon, LogoMark, WarningIcon } from "./icons";
import { DOCS_URL } from "./Shell";

const REMEMBER = "aether.keyId";

function storage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

/** Where to go after signing in: only paths on this origin. */
// Only a path on this console: browsers read "//host" and "/\host" as another
// site, so the result is checked as a URL rather than by its prefix.
function safeNext(next: string | null): string {
  if (!next?.startsWith("/")) return "/";
  try {
    const url = new URL(next, window.location.origin);
    if (url.origin !== window.location.origin || url.pathname.startsWith("/login")) return "/";
    return url.pathname + url.search + url.hash;
  } catch {
    return "/";
  }
}

function isLoopback(host: string): boolean {
  return host === "localhost" || host === "[::1]" || host.startsWith("127.");
}

export function SignIn() {
  const params = useSearchParams();
  const [keyId, setKeyId] = useState("");
  const [secret, setSecret] = useState("");
  const [show, setShow] = useState(false);
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [health, setHealth] = useState<Health | null>(null);
  const [plainHttp, setPlainHttp] = useState(false);

  useEffect(() => {
    const saved = storage()?.getItem(REMEMBER);
    if (saved) {
      setKeyId(saved);
      setRemember(true);
    }
    setPlainHttp(window.location.protocol === "http:" && !isLoopback(window.location.hostname));
    fetch("/api/v1/health")
      .then((r) => r.json() as Promise<Health>)
      .then(setHealth, () => setHealth({ status: "unavailable", version: "", tls: false }));
  }, []);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api<Session>("/login", { method: "POST", json: { access_key_id: keyId.trim(), secret_key: secret } });
      if (remember) storage()?.setItem(REMEMBER, keyId.trim());
      else storage()?.removeItem(REMEMBER);
      window.location.assign(safeNext(params.get("next")));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <main className="signin">
      <div className="signin-box">
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 14, textAlign: "center" }}>
          <div className="logo" style={{ width: 48, height: 48 }}>
            <LogoMark size={26} />
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <h1 style={{ fontSize: 24, fontWeight: 600, letterSpacing: "-0.02em" }}>Sign in to Aether</h1>
            <p className="muted">
              Use the root key or an admin key. The secret is checked once; this browser keeps only a session, which
              ends when you sign out or after 12 hours without use.
            </p>
          </div>
        </div>

        <form className="signin-card" onSubmit={submit}>
          <label className="label">
            Access key ID
            <span className="field field-lg">
              <input
                type="text"
                name="username"
                autoComplete="username"
                spellCheck={false}
                required
                placeholder="admin"
                value={keyId}
                onChange={(e) => setKeyId(e.target.value)}
                style={{ fontFamily: "var(--mono)" }}
              />
            </span>
          </label>
          <label className="label">
            Secret key
            <span className="field field-lg" style={{ paddingRight: 4 }}>
              <input
                type={show ? "text" : "password"}
                name="password"
                autoComplete="current-password"
                spellCheck={false}
                required
                placeholder="Secret key"
                value={secret}
                onChange={(e) => setSecret(e.target.value)}
                style={{ fontFamily: "var(--mono)" }}
              />
              <button
                type="button"
                className="btn btn-quiet btn-icon"
                style={{ width: 36, height: 36 }}
                aria-label={show ? "Hide secret key" : "Show secret key"}
                onClick={() => setShow(!show)}
              >
                {show ? <EyeOffIcon /> : <EyeIcon />}
              </button>
            </span>
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 10, minHeight: 32, fontSize: 13, color: "var(--muted)", cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={remember}
              onChange={(e) => setRemember(e.target.checked)}
              style={{ width: 18, height: 18, margin: 0 }}
            />
            Remember the key ID on this browser
          </label>
          {error && (
            <p className="error-text" role="alert">
              {error}
            </p>
          )}
          <button type="submit" className="btn btn-primary" style={{ height: 44 }} disabled={busy}>
            {busy ? "Signing in…" : "Sign in"}
          </button>
        </form>

        {plainHttp && (
          <div className="notice">
            <WarningIcon size={18} color="var(--warn)" />
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <span style={{ fontWeight: 500 }}>Warning: this connection is not encrypted</span>
              <span className="muted">
                The secret key and session travel in plain text. Reach the admin port through an SSH tunnel, or set{" "}
                <span className="mono">tls.admin = true</span>.
              </span>
            </div>
          </div>
        )}
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12, color: "var(--subtle)" }}>
        {health && (
          <>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span
                className="dot"
                style={{ width: 6, height: 6, background: health.status === "ready" ? "var(--ok)" : "var(--danger-text)" }}
              />
              {health.status === "ready" ? "Node ready" : "Node unavailable"}
            </span>
            {health.version && (
              <>
                <span className="sep">·</span>
                <span className="mono">v{health.version}</span>
              </>
            )}
            <span className="sep">·</span>
          </>
        )}
        <a href={DOCS_URL} target="_blank" rel="noreferrer" style={{ minHeight: 32, display: "flex", alignItems: "center" }}>
          Documentation
        </a>
      </div>
    </main>
  );
}
