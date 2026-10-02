"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { createContext, useContext, useEffect, useState, type ReactNode } from "react";

import { api, CONSOLE_HEADER, type Session, type Status } from "@/lib/api";
import { bytes, duration } from "@/lib/format";
import { useApi } from "@/lib/hooks";
import { BookIcon, BucketIcon, FolderIcon, KeyIcon, LogoMark, MenuIcon, OverviewIcon, SignOutIcon } from "./icons";

type ShellState = {
  session: Session;
  status: Status | undefined;
  refreshStatus: () => void;
};

const ShellContext = createContext<ShellState | null>(null);

export function useShell(): ShellState {
  const shell = useContext(ShellContext);
  if (!shell) throw new Error("useShell outside the console shell");
  return shell;
}

export const DOCS_URL = "https://github.com/aitoncumbi/Aeather/blob/main/README.md";

/** Every signed-in page: the sidebar, the session and the node's status. */
export function Shell({ children }: { children: ReactNode }) {
  const session = useApi<Session>("/session");
  const status = useApi<Status>(session.data ? "/status" : null, 15_000);
  const [menuOpen, setMenuOpen] = useState(false);
  const pathname = usePathname();

  useEffect(() => setMenuOpen(false), [pathname]);
  useEffect(() => {
    if (!menuOpen) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setMenuOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [menuOpen]);

  if (!session.data) {
    // Until the session is confirmed (or the redirect to sign-in happens).
    return (
      <div className="app" aria-busy="true">
        {session.error && session.error.status !== 401 && (
          <div className="signin">
            <p className="error-text">{session.error.message}</p>
          </div>
        )}
      </div>
    );
  }

  return (
    <ShellContext.Provider value={{ session: session.data, status: status.data, refreshStatus: status.reload }}>
      <div className="app">
        <div className="mtop">
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div className="logo" style={{ width: 28, height: 28 }}>
              <LogoMark size={16} />
            </div>
            <span style={{ fontWeight: 600 }}>Aether</span>
          </div>
          <button
            type="button"
            className="btn btn-surface btn-icon"
            style={{ width: 44, height: 44 }}
            aria-label="Open navigation"
            aria-expanded={menuOpen}
            aria-controls="sidebar"
            onClick={() => setMenuOpen(true)}
          >
            <MenuIcon size={18} />
          </button>
        </div>
        <div className="frame">
          <button
            type="button"
            className={`scrim${menuOpen ? " open" : ""}`}
            aria-label="Close navigation"
            tabIndex={menuOpen ? 0 : -1}
            onClick={() => setMenuOpen(false)}
          />
          <Sidebar session={session.data} status={status.data} open={menuOpen} />
          <main className="main" id="main">
            {children}
          </main>
        </div>
      </div>
    </ShellContext.Provider>
  );
}

function Sidebar({ session, status, open }: { session: Session; status: Status | undefined; open: boolean }) {
  const pathname = usePathname().replace(/\/$/, "") || "/";
  const current = (href: string) => (pathname === href ? "page" : undefined);
  const disk = diskShares(status);

  return (
    <aside id="sidebar" className={`side${open ? " open" : ""}`}>
      <div className="brand">
        <div className="logo">
          <LogoMark />
        </div>
        <div style={{ display: "flex", flexDirection: "column", flexGrow: 1 }}>
          <span style={{ fontSize: 15, fontWeight: 600, letterSpacing: "-0.01em" }}>Aether</span>
          <span style={{ fontSize: 12, color: "var(--subtle)" }}>Object storage console</span>
        </div>
      </div>
      <nav className="nav" aria-label="Console sections">
        <span className="nav-label">Storage</span>
        <Link href="/" aria-current={current("/")}>
          <OverviewIcon />
          <span className="grow">Overview</span>
        </Link>
        <Link href="/buckets/" aria-current={current("/buckets")}>
          <BucketIcon />
          <span className="grow">Buckets</span>
          {status && <span className="count">{status.buckets}</span>}
        </Link>
        <Link href="/objects/" aria-current={current("/objects")}>
          <FolderIcon />
          <span className="grow">Object browser</span>
        </Link>
        <span className="nav-label">Access</span>
        <Link href="/keys/" aria-current={current("/keys")}>
          <KeyIcon />
          <span className="grow">Access keys</span>
          {status && <span className="count">{status.access_keys}</span>}
        </Link>
        <a href={DOCS_URL} target="_blank" rel="noreferrer">
          <BookIcon />
          <span className="grow">Documentation</span>
        </a>
      </nav>
      <div style={{ flexGrow: 1 }} />
      {status && (
        <div className="side-status">
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span className="dot" style={{ background: "var(--ok)" }} />
            <span style={{ fontWeight: 500, flexGrow: 1 }}>Ready</span>
            <span className="mono subtle" style={{ fontSize: 12 }}>
              up {duration(status.uptime_secs)}
            </span>
          </div>
          {disk && (
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, color: "var(--muted)" }}>
                <span>Disk</span>
                <span className="mono">
                  {bytes(disk.used)} / {bytes(disk.total)}
                </span>
              </div>
              <DiskMeter status={status} />
            </div>
          )}
        </div>
      )}
      <div className="me">
        <div className="me-icon">
          <KeyIcon size={15} />
        </div>
        <div style={{ display: "flex", flexDirection: "column", flexGrow: 1, minWidth: 0 }}>
          <span className="mono ellipsis" style={{ fontSize: 13, fontWeight: 500 }} title={session.access_key_id}>
            {session.access_key_id}
          </span>
          <span className="ellipsis" style={{ fontSize: 12, color: "var(--subtle)" }}>
            {session.root ? "Root key" : `Admin key · ${session.name}`}
          </span>
        </div>
        <button type="button" className="btn btn-quiet btn-icon" aria-label="Sign out" title="Sign out" onClick={signOut}>
          <SignOutIcon />
        </button>
      </div>
    </aside>
  );
}

async function signOut() {
  try {
    await api("/logout", { method: "POST", headers: CONSOLE_HEADER });
  } finally {
    window.location.assign("/login/");
  }
}

/** Used, Aether's share of it, and the total of the data directory's disk. */
export function diskShares(status: Status | undefined) {
  if (!status?.disk_total_bytes || status.disk_available_bytes == null) return null;
  const total = status.disk_total_bytes;
  const used = Math.max(0, total - status.disk_available_bytes);
  const aether = Math.min(status.bytes, used);
  return { total, used, aether, other: used - aether, free: status.disk_available_bytes };
}

/** Aether's data, other files, free space: a three-part bar. */
export function DiskMeter({ status, large }: { status: Status; large?: boolean }) {
  const disk = diskShares(status);
  if (!disk) return null;
  const pct = (n: number) => `${((n / disk.total) * 100).toFixed(2)}%`;
  return (
    <div className={`meter${large ? " meter-lg" : ""}`} aria-hidden="true">
      <span style={{ width: pct(disk.aether), minWidth: disk.aether ? 2 : 0, background: "var(--accent)" }} />
      <span style={{ width: pct(disk.other), minWidth: disk.other ? 2 : 0, background: "#52525b" }} />
      <span style={{ flexGrow: 1, background: large ? "#1e1e22" : "var(--track)" }} />
    </div>
  );
}
