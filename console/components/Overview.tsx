"use client";

import Link from "next/link";
import { useState } from "react";

import { api, ApiError, type Bucket, type Stats, type StatsRange, type Status } from "@/lib/api";
import { ago, bytes, bytesParts, count, ms, percent, rate } from "@/lib/format";
import { useApi, useNow } from "@/lib/hooks";
import { BandwidthChart, LatencyChart, RequestsChart, Sparkline } from "./charts";
import {
  ActivityIcon,
  CheckCircleIcon,
  ChevronRightIcon,
  DriveIcon,
  FileIcon,
  RefreshIcon,
  ShieldCheckIcon,
  ShieldIcon,
  WarningIcon,
} from "./icons";
import { DiskMeter, diskShares } from "./Shell";
import { CopyButton, ErrorPanel, Figure, useToast } from "./ui";

const RANGES: StatsRange[] = ["1h", "24h", "7d"];

export function Overview() {
  const [range, setRange] = useState<StatsRange>("24h");
  const [live, setLive] = useState(true);
  const refresh = live ? 10_000 : undefined;
  const status = useApi<Status>("/status", refresh);
  const stats = useApi<Stats>(`/stats?range=${range}`, refresh);
  const buckets = useApi<Bucket[]>("/buckets", live ? 30_000 : undefined);

  if (status.error) return <ErrorPanel message={status.error.message} onRetry={status.reload} />;
  const s = status.data;

  return (
    <div className="page" style={{ gap: 24 }}>
      <div className="head">
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <h1 className="title">Overview</h1>
          {s && (
            <div className="subtitle">
              <span>Single node</span>
              <span className="sep">·</span>
              <span className="mono" title={s.s3_endpoint_configured ? "S3 endpoint (s3.public_url)" : "S3 endpoint, from this page's host; set s3.public_url to fix it"}>
                {s.s3_endpoint}
              </span>
              <CopyButton text={s.s3_endpoint} label="Copy S3 endpoint" size="xs" />
            </div>
          )}
        </div>
        <div className="head-actions">
          <div className="segmented" role="group" aria-label="Time range">
            {RANGES.map((r) => (
              <button key={r} type="button" aria-pressed={r === range} onClick={() => setRange(r)}>
                {r}
              </button>
            ))}
          </div>
          <button type="button" className="btn btn-surface" aria-pressed={live} onClick={() => setLive(!live)} style={{ fontSize: 13 }}>
            <span className="dot" style={{ width: 6, height: 6, background: live ? "var(--ok)" : "var(--sep)" }} />
            {live ? "Live" : "Paused"}
            {live && <span className="hide-sm subtle" style={{ fontWeight: 400 }}>· 10 s</span>}
          </button>
        </div>
      </div>

      <div className="stack">
        <div className="ruled kpis">
          <Kpis status={s} stats={stats.data} />
        </div>
        <div className="ruled g21">
          {stats.data ? <RequestsChart stats={stats.data} /> : <ChartPlaceholder error={stats.error} />}
          <StoragePanel status={s} />
        </div>
        <div className="ruled g11">
          {stats.data ? <BandwidthChart stats={stats.data} /> : <ChartPlaceholder error={stats.error} />}
          {stats.data ? <LatencyChart stats={stats.data} /> : <ChartPlaceholder error={stats.error} />}
        </div>
        <div className="ruled g11">
          <LargestBuckets buckets={buckets.data} />
          <Health status={s} onScrub={status.reload} />
        </div>
      </div>
    </div>
  );
}

function ChartPlaceholder({ error }: { error: ApiError | null }) {
  return (
    <section className="panel" style={{ minHeight: 320 }}>
      {error ? <ErrorPanel message={error.message} /> : <span className="caption">Loading…</span>}
    </section>
  );
}

function Kpis({ status, stats }: { status: Status | undefined; stats: Stats | undefined }) {
  const disk = diskShares(status);
  const [value, unit] = bytesParts(status?.bytes ?? 0);
  const change = stats?.objects_change;
  const day = stats?.day;
  const errors = day ? day.client_errors + day.server_errors : 0;
  const dayStart = stats ? stats.now - 24 * 3600_000 : 0;
  const window = day?.since && day.since > dayStart + 60_000 ? `since ${ago(day.since, stats!.now)}` : "in 24 h";
  return (
    <>
      <section className="kpi">
        <div className="kpi-label">
          <DriveIcon size={15} />
          Data stored
        </div>
        <Figure value={status ? value : "—"} unit={status ? unit : undefined} />
        {status && <DiskMeter status={status} />}
        <div className="caption">
          {disk ? `${percent(disk.aether, disk.total)} of a ${bytes(disk.total)} disk · ${bytes(disk.free)} free` : "Disk size unknown"}
        </div>
      </section>
      <section className="kpi">
        <div className="kpi-label">
          <FileIcon size={15} />
          Objects
        </div>
        <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 12 }}>
          <Figure value={status ? count(status.objects) : "—"} />
          <Sparkline values={stats?.objects.map((p) => p.objects) ?? []} />
        </div>
        <div className="caption">
          {change ? `${signed(change.to - change.from)} ${change.since > dayStart + 60_000 ? `since ${ago(change.since, stats!.now)}` : "in the last 24 h"}` : "Counting…"}
          {status && ` · ${count(status.buckets)} bucket${status.buckets === 1 ? "" : "s"}`}
        </div>
      </section>
      <section className="kpi">
        <div className="kpi-label">
          <ActivityIcon size={15} />
          Requests now
        </div>
        <Figure value={stats ? rate(stats.current.requests_per_sec) : "—"} unit="/s" />
        <div className="caption">
          {stats ? `p99 latency ${ms(stats.current.p99_ms)} · p50 ${ms(stats.current.p50_ms)}` : "Last minute"}
        </div>
      </section>
      <section className="kpi">
        <div className="kpi-label">
          <WarningIcon size={15} />
          Error rate
        </div>
        <Figure value={day && day.requests ? percent(errors, day.requests).replace("%", "") : "—"} unit={day && day.requests ? "%" : undefined} />
        <div className="caption">
          {day && day.requests
            ? `4xx ${percent(day.client_errors, day.requests)} · 5xx ${percent(day.server_errors, day.requests)} (${count(day.server_errors)} ${window})`
            : `No S3 requests ${window}`}
        </div>
      </section>
    </>
  );
}

function signed(n: number): string {
  return `${n >= 0 ? "+" : "−"}${count(Math.abs(n))}`;
}

function StoragePanel({ status }: { status: Status | undefined }) {
  const disk = diskShares(status);
  return (
    <section className="panel" aria-labelledby="disk-title" style={{ gap: 18 }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <h2 id="disk-title" className="panel-title">
          Storage
        </h2>
        <span className="panel-sub wrap-any">
          Data directory <span className="mono">{status?.data_dir ?? "…"}</span>
        </span>
      </div>
      {status && (
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {disk ? (
            <>
              <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                <span style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em" }}>{bytes(disk.used)}</span>
                <span className="subtle" style={{ fontSize: 13 }}>
                  of {bytes(disk.total)} used
                </span>
              </div>
              <DiskMeter status={status} large />
              <div style={{ display: "flex", flexDirection: "column", gap: 8, fontSize: 13 }}>
                <LegendRow color="var(--accent)" label="Aether objects" value={bytes(disk.aether)} />
                <LegendRow color="#52525b" label="Other files on the disk" value={bytes(disk.other)} />
                <LegendRow color="#1e1e22" label="Free" value={bytes(disk.free)} />
              </div>
            </>
          ) : (
            <span className="caption">The file system did not report its size.</span>
          )}
        </div>
      )}
      <div style={{ borderTop: "1px solid var(--rule)" }} />
      {status && (
        <dl className="dl2">
          <div>
            <dt>Blocks on disk</dt>
            <dd className="tnum">{count(status.blocks)}</dd>
          </div>
          <div>
            <dt>Stored inline</dt>
            <dd>Objects up to {binarySize(status.inline_threshold)}</dd>
          </div>
          <div>
            <dt>Block size</dt>
            <dd>{binarySize(status.block_size)}</dd>
          </div>
          <div>
            <dt>Awaiting GC</dt>
            <dd className="tnum">
              {count(status.gc_pending_blocks)} block{status.gc_pending_blocks === 1 ? "" : "s"}
            </dd>
          </div>
        </dl>
      )}
    </section>
  );
}

/** Configured sizes, which are powers of two: `4 KiB`, `64 MiB`. */
function binarySize(n: number): string {
  if (n >= 1024 * 1024 && n % (1024 * 1024) === 0) return `${n / (1024 * 1024)} MiB`;
  if (n >= 1024 && n % 1024 === 0) return `${n / 1024} KiB`;
  return bytes(n);
}

function LegendRow({ color, label, value }: { color: string; label: string; value: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <span className="swatch" style={{ background: color }} />
      <span className="muted" style={{ flexGrow: 1 }}>
        {label}
      </span>
      <span className="tnum">{value}</span>
    </div>
  );
}

function LargestBuckets({ buckets }: { buckets: Bucket[] | undefined }) {
  const top = [...(buckets ?? [])].sort((a, b) => b.bytes - a.bytes).slice(0, 6);
  const max = top[0]?.bytes || 1;
  return (
    <section className="panel" aria-labelledby="top-title" style={{ gap: 14 }}>
      <div className="panel-head" style={{ alignItems: "center" }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <h2 id="top-title" className="panel-title">
            Largest buckets
          </h2>
          <span className="panel-sub">By bytes stored</span>
        </div>
        <Link href="/buckets/" style={{ fontSize: 13, display: "flex", alignItems: "center", gap: 4, minHeight: 32 }}>
          All buckets
          <ChevronRightIcon size={14} />
        </Link>
      </div>
      {buckets && top.length === 0 && <p className="caption">No buckets yet.</p>}
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {top.map((b) => (
          <Link key={b.name} href={`/buckets/?name=${encodeURIComponent(b.name)}`} className="top-row" style={{ color: "var(--text)" }}>
            <span className="mono ellipsis" style={{ fontSize: 13 }}>
              {b.name}
            </span>
            <div className="hbar">
              <div style={{ width: `${((b.bytes / max) * 100).toFixed(1)}%` }} />
            </div>
            <span className="tnum right" style={{ fontSize: 13 }}>
              {bytes(b.bytes)}
            </span>
          </Link>
        ))}
      </div>
    </section>
  );
}

function Health({ status, onScrub }: { status: Status | undefined; onScrub: () => void }) {
  const now = useNow();
  const notify = useToast();
  const [starting, setStarting] = useState(false);
  const scrub = status?.scrub;
  const corrupt = scrub?.corrupt.length ?? 0;
  const running = scrub && scrub.finished_at == null;
  const loopback = typeof window !== "undefined" && /^(localhost|127\.|\[::1\])/.test(window.location.hostname);

  async function startScrub() {
    setStarting(true);
    try {
      await api("/scrub", { method: "POST" });
      notify("Scrub started. Progress shows here.");
      onScrub();
    } catch (e) {
      notify(e instanceof Error ? e.message : String(e), "error");
    } finally {
      setStarting(false);
    }
  }

  return (
    <section className="panel" aria-labelledby="health-title" style={{ gap: 14 }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <h2 id="health-title" className="panel-title">
          Health and maintenance
        </h2>
        <span className="panel-sub">Checks the node runs on its own</span>
      </div>
      {status && (
        <div style={{ display: "flex", flexDirection: "column" }}>
          <div className="health-row">
            <CheckCircleIcon size={18} color="var(--ok)" />
            <div className="row-text">
              <span className="row-title">Live and ready</span>
              <span className="row-hint">Metadata store answered in {ms(status.metadata_latency_ms)}</span>
            </div>
            <span className="mono muted hide-sm" style={{ fontSize: 12 }}>
              /_aether/health
            </span>
          </div>
          <div className="health-row">
            {corrupt > 0 ? (
              <WarningIcon size={18} color="var(--danger-text)" />
            ) : (
              <ShieldCheckIcon size={18} color={scrub?.finished_at ? "var(--ok)" : "var(--muted)"} />
            )}
            <div className="row-text">
              <span className="row-title">
                {!scrub
                  ? "Integrity scrub: not run yet"
                  : running
                    ? "Integrity scrub: running"
                    : corrupt > 0
                      ? `Integrity scrub: ${count(corrupt)} damaged block${corrupt === 1 ? "" : "s"}`
                      : "Integrity scrub: no corrupt blocks"}
              </span>
              <span className="row-hint">
                {!scrub
                  ? "Every block is re-read and checked against its CRC32C once a week."
                  : running
                    ? `${count(scrub.blocks_checked)} blocks and ${bytes(scrub.bytes_checked)} checked so far`
                    : `Finished ${ago(scrub.finished_at!, now)} · ${count(scrub.blocks_checked)} blocks and ${bytes(scrub.bytes_checked)} checked against CRC32C`}
              </span>
              {corrupt > 0 && (
                <span className="row-hint" style={{ color: "var(--danger-text)" }}>
                  First: {scrub!.corrupt[0]!.bucket}/{scrub!.corrupt[0]!.key} ({scrub!.corrupt[0]!.problem}). See the server log for all.
                </span>
              )}
            </div>
            <button type="button" className="btn btn-sm" disabled={!!running || starting} onClick={startScrub}>
              {running ? "Running" : "Scrub now"}
            </button>
          </div>
          <div className="health-row">
            <RefreshIcon size={18} color="var(--muted)" />
            <div className="row-text">
              <span className="row-title">
                Garbage collection: {count(status.gc_pending_blocks)} block{status.gc_pending_blocks === 1 ? "" : "s"} queued
              </span>
              <span className="row-hint">
                Deleted after a {Math.round(status.gc_grace_secs / 60)} min grace period, so running downloads can finish
              </span>
            </div>
          </div>
          <div className="health-row">
            <ShieldIcon size={18} color={status.encryption_key ? "var(--ok)" : "var(--muted)"} />
            <div className="row-text">
              <span className="row-title">Encryption at rest: {status.encryption_key ? "on" : "off"}</span>
              <span className="row-hint">
                {status.encryption_key ? (
                  <>
                    New objects are encrypted (SSE-S3) with master key <span className="mono">{status.encryption_key}</span>.
                  </>
                ) : (
                  <>
                    Set <span className="mono">encryption.master_key</span> to encrypt new objects.
                  </>
                )}
              </span>
            </div>
          </div>
          {!status.admin_tls && (
            <div className="health-row">
              <WarningIcon size={18} color={loopback ? "var(--muted)" : "var(--warn)"} />
              <div className="row-text">
                <span className="row-title">
                  {loopback ? "Admin API runs over plain HTTP" : "Warning: admin API runs over plain HTTP"}
                </span>
                <span className="row-hint">
                  {loopback ? "Fine over loopback or an SSH tunnel. " : "Sessions and secrets cross the network unencrypted. "}
                  Set <span className="mono">tls.admin = true</span> before exposing it.
                </span>
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
