// Number, size and time formatting shared by every page. Sizes use decimal
// units (1 GB = 10^9 bytes), like most S3 tools.

const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];

export function bytes(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  if (n < 1000) return `${n} B`;
  let value = n;
  let unit = 0;
  while (value >= 1000 && unit < UNITS.length - 1) {
    value /= 1000;
    unit += 1;
  }
  const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${UNITS[unit]}`;
}

/** `bytes()` split into number and unit, for large figures. */
export function bytesParts(n: number): [string, string] {
  const [value, unit] = bytes(n).split(" ");
  return [value ?? "0", unit ?? "B"];
}

export function count(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return Math.round(n).toLocaleString("en-US");
}

export function rate(n: number): string {
  if (n >= 100) return count(n);
  if (n >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

export function ms(n: number | null | undefined): string {
  if (n == null) return "—";
  if (n >= 1000) return `${(n / 1000).toFixed(2)} s`;
  if (n >= 100) return `${Math.round(n)} ms`;
  return `${n.toFixed(1)} ms`;
}

export function percent(part: number, whole: number): string {
  if (!whole) return "—";
  const p = (part / whole) * 100;
  return `${p < 1 && p > 0 ? p.toFixed(2) : p.toFixed(1)}%`;
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const pad = (n: number) => String(n).padStart(2, "0");

function toDate(t: number | string): Date {
  return typeof t === "number" ? new Date(t) : new Date(t);
}

/** `Jan 12, 2026` */
export function date(t: number | string): string {
  const d = toDate(t);
  return `${MONTHS[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
}

/** `Sep 30, 2026, 17:42` */
export function dateTime(t: number | string): string {
  const d = toDate(t);
  return `${date(d.getTime())}, ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** `17:42` */
export function time(t: number): string {
  const d = new Date(t);
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** `Sep 30` */
export function day(t: number): string {
  const d = new Date(t);
  return `${MONTHS[d.getMonth()]} ${d.getDate()}`;
}

/** `12d 4h`, `3h 20m`, `45s` */
export function duration(secs: number): string {
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${Math.floor(secs)}s`;
}

/** `3 h ago` */
export function ago(t: number, now = Date.now()): string {
  const secs = Math.max(0, (now - t) / 1000);
  if (secs < 60) return "just now";
  if (secs < 3600) return `${Math.floor(secs / 60)} min ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)} h ago`;
  return `${Math.floor(secs / 86400)} d ago`;
}

const KINDS: Record<string, string> = {
  pdf: "PDF",
  csv: "CSV",
  tsv: "TSV",
  json: "JSON",
  parquet: "Parquet",
  xlsx: "Spreadsheet",
  xls: "Spreadsheet",
  ods: "Spreadsheet",
  docx: "Document",
  doc: "Document",
  md: "Markdown",
  txt: "Text",
  log: "Log",
  html: "HTML",
  xml: "XML",
  yaml: "YAML",
  yml: "YAML",
  png: "Image",
  jpg: "Image",
  jpeg: "Image",
  gif: "Image",
  webp: "Image",
  svg: "Image",
  mp4: "Video",
  mov: "Video",
  mp3: "Audio",
  zip: "Archive",
  gz: "Archive",
  tgz: "Archive",
  tar: "Archive",
  zst: "Archive",
  sql: "SQL",
  bin: "Binary",
};

export function extension(name: string): string {
  const dot = name.lastIndexOf(".");
  return dot > 0 ? name.slice(dot + 1).toLowerCase() : "";
}

export function kind(name: string): string {
  return KINDS[extension(name)] ?? "File";
}

/** The last path segment of a key or prefix. */
export function baseName(key: string): string {
  const trimmed = key.endsWith("/") ? key.slice(0, -1) : key;
  const slash = trimmed.lastIndexOf("/");
  return (slash >= 0 ? trimmed.slice(slash + 1) : trimmed) + (key.endsWith("/") ? "/" : "");
}
