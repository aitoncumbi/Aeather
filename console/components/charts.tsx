"use client";

import { useId, useState, type MouseEvent, type ReactNode } from "react";

import type { Stats, StatsPoint } from "@/lib/api";
import { bytes, dateTime, day, ms, rate, time } from "@/lib/format";

// Series colors, in fixed order (validated for color-vision deficiencies
// against the dark surface): blue, orange, green.
export const SERIES = ["#3987E5", "#D95926", "#199E70"] as const;

const W = 1000;

/** A round axis maximum at or above `v`. */
function niceMax(v: number): number {
  if (!(v > 0)) return 1;
  const exp = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) {
    if (m * exp >= v) return m * exp;
  }
  return 10 * exp;
}

function tickLabel(v: number): string {
  if (v >= 1000) return `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k`;
  if (v >= 10 || v === 0) return String(Math.round(v));
  return v.toFixed(v < 1 ? 2 : 1).replace(/\.?0+$/, "");
}

function linePath(points: [number, number][]): string {
  return points.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
}

type Axis = { start: number; now: number; label: (t: number) => string };

function axisOf(stats: Stats): Axis {
  return { start: stats.start, now: stats.now, label: stats.range === "7d" ? day : time };
}

const xOf = (axis: Axis, t: number) => ((t - axis.start) / Math.max(1, axis.now - axis.start)) * W;
const pctOf = (axis: Axis, t: number) => `${((xOf(axis, t) / W) * 100).toFixed(2)}%`;

function XLabels({ axis, style }: { axis: Axis; style?: React.CSSProperties }) {
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => axis.start + f * (axis.now - axis.start));
  return (
    <div className="axis-x" style={style} aria-hidden="true">
      {ticks.map((t) => (
        <span key={t}>{axis.label(t)}</span>
      ))}
    </div>
  );
}

function YAxis({ max, height }: { max: number; height: number }) {
  return (
    <div className="axis-y" style={{ height }} aria-hidden="true">
      {[0, 1, 2, 3, 4].map((i) => (
        <span key={i} style={{ top: (i / 4) * height - 8 }}>
          {tickLabel(max * (1 - i / 4))}
        </span>
      ))}
    </div>
  );
}

function Gridlines({ height }: { height: number }) {
  return (
    <>
      {[0, 1, 2, 3, 4].map((i) => (
        <div key={i} className="gridline" style={{ top: (i / 4) * height }} />
      ))}
    </>
  );
}

/** Index of the point nearest the pointer. */
function nearest(e: MouseEvent<HTMLDivElement>, axis: Axis, ts: number[]): number | null {
  if (ts.length === 0) return null;
  const rect = e.currentTarget.getBoundingClientRect();
  const f = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
  const t = axis.start + f * (axis.now - axis.start);
  let best = 0;
  for (let i = 1; i < ts.length; i++) {
    if (Math.abs(ts[i]! - t) < Math.abs(ts[best]! - t)) best = i;
  }
  return best;
}

/** "· since the server started 6 min ago", when the history is shorter than the range. */
export function sinceNote(stats: Stats): string {
  if (!stats.history_start || stats.history_start <= stats.start) return "";
  const mins = Math.max(1, Math.round((stats.now - stats.history_start) / 60_000));
  const span = mins < 120 ? `${mins} min` : `${Math.round(mins / 60)} h`;
  return ` · history since the server started ${span} ago`;
}

/** A lone sample has no line to draw: mark it instead. */
function Dots({ series }: { series: { key: string; color: string; xy: [number, number][] }[] }) {
  return (
    <>
      {series
        .filter((s) => s.xy.length === 1)
        .map((s) => (
          <span
            key={s.key}
            style={{ position: "absolute", left: `${(s.xy[0]![0] / W) * 100}%`, top: s.xy[0]![1], width: 6, height: 6, margin: "-3px 0 0 -3px", background: s.color }}
          />
        ))}
    </>
  );
}

function Empty({ stats }: { stats: Stats }) {
  return (
    <div className="chart-empty">
      {stats.history_start ? "No samples in this range yet." : "Collecting the first samples (every 10 seconds)…"}
    </div>
  );
}

export function TableToggle({ table, onToggle }: { table: boolean; onToggle: () => void }) {
  return (
    <button type="button" className="btn btn-xs" aria-pressed={table} onClick={onToggle} style={{ color: "var(--muted)" }}>
      {table ? "View as chart" : "View as table"}
    </button>
  );
}

export function ChartHead({ title, sub, id, right }: { title: string; sub: ReactNode; id: string; right?: ReactNode }) {
  return (
    <div className="panel-head">
      <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <h2 id={id} className="panel-title">
          {title}
        </h2>
        <span className="panel-sub">{sub}</span>
      </div>
      {right}
    </div>
  );
}

// ---- requests ----

const KINDS = [
  { key: "reads", name: "Reads", hint: "GetObject, HeadObject" },
  { key: "writes", name: "Writes", hint: "Put, copy, delete, multipart" },
  { key: "lists", name: "Lists", hint: "ListObjects, ListBuckets, …" },
] as const;

export function RequestsChart({ stats }: { stats: Stats }) {
  const id = useId();
  const [hover, setHover] = useState<number | null>(null);
  const [table, setTable] = useState(false);
  const H = 220;
  const axis = axisOf(stats);
  const points = stats.points;
  const max = niceMax(Math.max(0, ...points.flatMap((p) => [p.reads, p.writes, p.lists])) * 1.08);
  const series = KINDS.map((k, i) => ({
    ...k,
    color: SERIES[i]!,
    xy: points.map((p) => [xOf(axis, p.t), H - (p[k.key] / max) * H] as [number, number]),
  }));
  const at = hover ?? points.length - 1;
  const atPoint: StatsPoint | undefined = points[at];
  const area = (d: string, xy: [number, number][]) =>
    xy.length ? `${d} L${xy[xy.length - 1]![0].toFixed(1)} ${H} L${xy[0]![0].toFixed(1)} ${H} Z` : "";

  // Direct labels at the line ends, nudged apart.
  const ends = series
    .filter((s) => s.xy.length)
    .map((s) => ({ name: s.name, y: s.xy[s.xy.length - 1]![1] }))
    .sort((a, b) => a.y - b.y);
  for (let i = 1; i < ends.length; i++) {
    if (ends[i]!.y - ends[i - 1]!.y < 16) ends[i]!.y = ends[i - 1]!.y + 16;
  }

  return (
    <section className="panel" aria-labelledby={`${id}-t`}>
      <ChartHead
        id={`${id}-t`}
        title="Requests"
        sub={`Per second, by kind of S3 operation${sinceNote(stats)}`}
        right={<TableToggle table={table} onToggle={() => setTable(!table)} />}
      />
      <div className="legend">
        {series.map((s) => (
          <div key={s.key} className="legend-item" title={s.hint}>
            <span className="swatch" style={{ background: s.color }} />
            <span className="muted">{s.name}</span>
            <span className="tnum" style={{ fontWeight: 500 }}>
              {atPoint ? rate(atPoint[s.key]) : "—"}
            </span>
          </div>
        ))}
      </div>
      {table ? (
        <PointsTable stats={stats} />
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          <div style={{ display: "flex" }}>
            <YAxis max={max} height={H} />
            <div className="plot" style={{ height: H }}>
              <Gridlines height={H} />
              <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden="true" style={{ position: "absolute", inset: 0, width: "100%", height: "100%", overflow: "visible" }}>
                <defs>
                  {series.slice(0, 2).map((s) => (
                    <linearGradient key={s.key} id={`${id}-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0" stopColor={s.color} stopOpacity={s.key === "reads" ? 0.22 : 0.16} />
                      <stop offset="1" stopColor={s.color} stopOpacity="0" />
                    </linearGradient>
                  ))}
                </defs>
                {series.slice(0, 2).map((s) => (
                  <path key={s.key} d={area(linePath(s.xy), s.xy)} fill={`url(#${id}-${s.key})`} />
                ))}
                {[...series].reverse().map((s) => (
                  <path key={s.key} d={linePath(s.xy)} fill="none" stroke={s.color} strokeWidth={2} vectorEffect="non-scaling-stroke" />
                ))}
              </svg>
              {points.length === 0 && <Empty stats={stats} />}
              <Dots series={series} />
              {hover != null && atPoint && (
                <>
                  <div className="crosshair" style={{ left: pctOf(axis, atPoint.t) }} />
                  {series.map((s) => (
                    <span key={s.key} className="marker" style={{ left: pctOf(axis, atPoint.t), top: s.xy[at]![1], background: s.color }} />
                  ))}
                  <div
                    className="tooltip"
                    style={{
                      left: pctOf(axis, atPoint.t),
                      transform: xOf(axis, atPoint.t) > W * 0.62 ? "translateX(calc(-100% - 12px))" : "translateX(12px)",
                    }}
                  >
                    <span className="muted" style={{ fontSize: 12 }}>
                      {dateTime(atPoint.t)}
                    </span>
                    {series.map((s) => (
                      <div key={s.key} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
                        <span className="dot" style={{ background: s.color }} />
                        <span className="muted" style={{ flexGrow: 1 }}>
                          {s.name}
                        </span>
                        <span className="tnum" style={{ fontWeight: 500 }}>
                          {rate(atPoint[s.key])}
                        </span>
                      </div>
                    ))}
                    <div style={{ display: "flex", justifyContent: "space-between", borderTop: "1px solid var(--btn-hover)", paddingTop: 6, fontSize: 13 }}>
                      <span className="muted">Total, incl. other</span>
                      <span className="tnum" style={{ fontWeight: 600 }}>
                        {rate(atPoint.reads + atPoint.writes + atPoint.lists + atPoint.other)} /s
                      </span>
                    </div>
                  </div>
                </>
              )}
              <div
                style={{ position: "absolute", inset: 0, cursor: "crosshair" }}
                onMouseMove={(e) => setHover(nearest(e, axis, points.map((p) => p.t)))}
                onMouseLeave={() => setHover(null)}
              />
            </div>
            <div style={{ width: 52, flexShrink: 0, position: "relative", height: H }} aria-hidden="true">
              {ends.map((e) => (
                <span key={e.name} style={{ position: "absolute", left: 10, top: e.y - 9, fontSize: 12, color: "var(--muted)", whiteSpace: "nowrap" }}>
                  {e.name}
                </span>
              ))}
            </div>
          </div>
          <XLabels axis={axis} style={{ margin: "0 52px 0 40px" }} />
        </div>
      )}
    </section>
  );
}

function PointsTable({ stats }: { stats: Stats }) {
  return (
    <div className="table-scroll">
      <table className="data">
        <caption className="sr-only">Requests per second and latency over time</caption>
        <thead>
          <tr>
            <th scope="col">Time</th>
            <th scope="col">Reads/s</th>
            <th scope="col">Writes/s</th>
            <th scope="col">Lists/s</th>
            <th scope="col">Other/s</th>
            <th scope="col">p50</th>
            <th scope="col">p99</th>
          </tr>
        </thead>
        <tbody>
          {[...stats.points].reverse().map((p) => (
            <tr key={p.t}>
              <td>{dateTime(p.t)}</td>
              <td>{rate(p.reads)}</td>
              <td>{rate(p.writes)}</td>
              <td>{rate(p.lists)}</td>
              <td>{rate(p.other)}</td>
              <td>{ms(p.p50_ms)}</td>
              <td>{ms(p.p99_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {stats.points.length === 0 && <p className="caption" style={{ padding: 8 }}>No samples yet.</p>}
    </div>
  );
}

// ---- latency ----

export function LatencyChart({ stats }: { stats: Stats }) {
  const id = useId();
  const [hover, setHover] = useState<number | null>(null);
  const [table, setTable] = useState(false);
  const H = 184;
  const axis = axisOf(stats);
  const points = stats.points.filter((p) => p.p50_ms != null);
  const max = niceMax(Math.max(0, ...points.map((p) => p.p99_ms ?? 0)) * 1.1);
  const y = (v: number | null) => H - ((v ?? 0) / max) * H;
  const p50 = points.map((p) => [xOf(axis, p.t), y(p.p50_ms)] as [number, number]);
  const p99 = points.map((p) => [xOf(axis, p.t), y(p.p99_ms)] as [number, number]);
  const at = hover != null ? points[hover] : undefined;
  return (
    <section className="panel" aria-labelledby={`${id}-t`}>
      <ChartHead
        id={`${id}-t`}
        title="Latency"
        sub={`Time to response headers, all S3 operations${sinceNote(stats)}`}
        right={
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4, fontSize: 13 }}>
            <span className="muted" style={{ fontSize: 12 }}>
              {at ? dateTime(at.t) : "Last 5 minutes"}
            </span>
            <div className="legend" style={{ gap: 14 }}>
              <span className="legend-item" style={{ gap: 6 }}>
                <span className="swatch" style={{ background: SERIES[0] }} />
                <span className="muted">p50</span>
                <span className="tnum" style={{ fontWeight: 500 }}>
                  {ms(at ? at.p50_ms : stats.current.p50_ms)}
                </span>
              </span>
              <span className="legend-item" style={{ gap: 6 }}>
                <span className="swatch" style={{ background: SERIES[1] }} />
                <span className="muted">p99</span>
                <span className="tnum" style={{ fontWeight: 500 }}>
                  {ms(at ? at.p99_ms : stats.current.p99_ms)}
                </span>
              </span>
            </div>
          </div>
        }
      />
      {table ? (
        <PointsTable stats={stats} />
      ) : (
        <>
          <div style={{ display: "flex" }}>
            <YAxis max={max} height={H} />
            <div className="plot" style={{ height: H }}>
              <Gridlines height={H} />
              <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" aria-hidden="true" style={{ position: "absolute", inset: 0, width: "100%", height: "100%", overflow: "visible" }}>
                <path d={linePath(p99)} fill="none" stroke={SERIES[1]} strokeWidth={2} vectorEffect="non-scaling-stroke" />
                <path d={linePath(p50)} fill="none" stroke={SERIES[0]} strokeWidth={2} vectorEffect="non-scaling-stroke" />
              </svg>
              {points.length === 0 && <Empty stats={stats} />}
              <Dots
                series={[
                  { key: "p50", color: SERIES[0], xy: p50 },
                  { key: "p99", color: SERIES[1], xy: p99 },
                ]}
              />
              {at && hover != null && (
                <>
                  <div className="crosshair" style={{ left: pctOf(axis, at.t) }} />
                  <span className="marker" style={{ left: pctOf(axis, at.t), top: p99[hover]![1], background: SERIES[1] }} />
                  <span className="marker" style={{ left: pctOf(axis, at.t), top: p50[hover]![1], background: SERIES[0] }} />
                </>
              )}
              <div
                style={{ position: "absolute", inset: 0, cursor: "crosshair" }}
                onMouseMove={(e) => setHover(nearest(e, axis, points.map((p) => p.t)))}
                onMouseLeave={() => setHover(null)}
              />
            </div>
          </div>
          <XLabels axis={axis} style={{ marginLeft: 40 }} />
        </>
      )}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span className="caption">Percentiles of the S3 requests in each interval, in ms</span>
        <TableToggle table={table} onToggle={() => setTable(!table)} />
      </div>
    </section>
  );
}

// ---- bandwidth ----

export function BandwidthChart({ stats }: { stats: Stats }) {
  const id = useId();
  const [hover, setHover] = useState<number | null>(null);
  const [table, setTable] = useState(false);
  const bars = stats.bars;
  const max = Math.max(1, ...bars.map((b) => Math.max(b.bytes_in, b.bytes_out)));
  const half = 90;
  const totalIn = bars.reduce((a, b) => a + b.bytes_in, 0);
  const totalOut = bars.reduce((a, b) => a + b.bytes_out, 0);
  const at = hover != null ? bars[hover] : undefined;
  const label = stats.range === "7d" ? dateTime : time;
  const per = stats.bar_secs >= 3600 ? `${stats.bar_secs / 3600} hour${stats.bar_secs === 3600 ? "" : "s"}` : `${stats.bar_secs / 60} minutes`;
  return (
    <section className="panel" aria-labelledby={`${id}-t`}>
      <ChartHead
        id={`${id}-t`}
        title="Bandwidth"
        sub={`Object data per ${per}${sinceNote(stats)}`}
        right={
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4, fontSize: 13 }}>
            <span className="muted" style={{ fontSize: 12 }}>
              {at ? `${dateTime(at.start)} – ${time(at.end)}` : `Total, last ${stats.range}`}
            </span>
            <div className="legend" style={{ gap: 14 }}>
              <span className="legend-item" style={{ gap: 6 }}>
                <span className="swatch" style={{ background: SERIES[0] }} />
                <span className="muted">In</span>
                <span className="tnum" style={{ fontWeight: 500 }}>
                  {bytes(at ? at.bytes_in : totalIn)}
                </span>
              </span>
              <span className="legend-item" style={{ gap: 6 }}>
                <span className="swatch" style={{ background: SERIES[1] }} />
                <span className="muted">Out</span>
                <span className="tnum" style={{ fontWeight: 500 }}>
                  {bytes(at ? at.bytes_out : totalOut)}
                </span>
              </span>
            </div>
          </div>
        }
      />
      {table ? (
        <div className="table-scroll">
          <table className="data">
            <caption className="sr-only">Bytes received and sent per interval</caption>
            <thead>
              <tr>
                <th scope="col">From</th>
                <th scope="col">In</th>
                <th scope="col">Out</th>
              </tr>
            </thead>
            <tbody>
              {[...bars].reverse().map((b) => (
                <tr key={b.start}>
                  <td>{dateTime(b.start)}</td>
                  <td>{b.covered ? bytes(b.bytes_in) : "—"}</td>
                  <td>{b.covered ? bytes(b.bytes_out) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <>
          <div style={{ display: "flex", alignItems: "stretch", gap: 2, height: half * 2 + 4 }} onMouseLeave={() => setHover(null)}>
            {bars.map((b, i) => (
              <div
                key={b.start}
                onMouseEnter={() => setHover(i)}
                style={{ flex: "1 1 0", minWidth: 0, display: "flex", flexDirection: "column", padding: "0 1px", background: hover === i ? "#1c1c21" : "transparent" }}
              >
                <div style={{ height: half, display: "flex", alignItems: "flex-end" }}>
                  {b.covered && <span style={{ width: "100%", height: Math.max(2, (b.bytes_in / max) * half), background: SERIES[0] }} />}
                </div>
                <div style={{ height: 4 }} />
                <div style={{ height: half, display: "flex", alignItems: "flex-start" }}>
                  {b.covered && <span style={{ width: "100%", height: Math.max(2, (b.bytes_out / max) * half), background: SERIES[1] }} />}
                </div>
              </div>
            ))}
          </div>
          <div className="axis-x" aria-hidden="true">
            {[0, 1, 2, 3].map((k) => {
              const bar = bars[Math.round((k * (bars.length - 1)) / 3)];
              return <span key={k}>{bar ? label(bar.start) : ""}</span>;
            })}
          </div>
        </>
      )}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span className="caption">In above the line, out below</span>
        <TableToggle table={table} onToggle={() => setTable(!table)} />
      </div>
    </section>
  );
}

// ---- sparkline ----

export function Sparkline({ values }: { values: number[] }) {
  if (values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const xy = values.map((v, i) => [1 + (i / (values.length - 1)) * 70, 25 - ((v - min) / span) * 21] as [number, number]);
  return (
    <svg width="72" height="28" viewBox="0 0 72 28" fill="none" aria-hidden="true" style={{ flexShrink: 0, marginBottom: 6 }}>
      <path d={linePath(xy)} stroke={SERIES[0]} strokeWidth={2} />
    </svg>
  );
}
