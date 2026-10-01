// Starts an aether server with a fresh data directory and a few buckets,
// objects and keys, and stops it when the run ends. The binary must embed this
// console: `npm run build`, then `cargo build --release` in aether_backend.
//
// Environment: AETHER (the binary), E2E_S3_PORT and E2E_ADMIN_PORT,
// KEEP_LOG (a path to keep the server log at).

import { spawn } from "node:child_process";
import { closeSync, existsSync, mkdtempSync, openSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ADMIN_PORT, ADMIN_URL, ROOT, S3_PORT, adminHeaders } from "./server";

export default async function globalSetup() {
  const binary = resolve(process.env.AETHER ?? join(__dirname, "../../aether_backend/target/release/aether"));
  if (!existsSync(binary)) {
    throw new Error(`no aether binary at ${binary}: run \`npm run build\`, then \`cargo build --release\` in aether_backend`);
  }

  const dataDir = mkdtempSync(join(tmpdir(), "aether-console-e2e-"));
  const logPath = process.env.KEEP_LOG ? resolve(process.env.KEEP_LOG) : join(dataDir, "server.log");
  const log = openSync(logPath, "w");
  const server = spawn(binary, ["serve"], {
    env: {
      ...process.env,
      AETHER_STORAGE__DATA_DIR: join(dataDir, "data"),
      AETHER_S3__LISTEN: `127.0.0.1:${S3_PORT}`,
      AETHER_ADMIN__LISTEN: `127.0.0.1:${ADMIN_PORT}`,
      AETHER_S3__ACCESS_KEY: ROOT.id,
      AETHER_S3__SECRET_KEY: ROOT.secret,
    },
    stdio: ["ignore", log, log],
  });
  closeSync(log);
  let exited = false;
  server.once("exit", () => (exited = true));

  const stop = async () => {
    if (!exited) {
      const done = new Promise((r) => server.once("exit", r));
      server.kill("SIGTERM");
      await done;
    }
    rmSync(dataDir, { recursive: true, force: true });
  };

  try {
    await waitForHealth(() => exited);
    await seed();
  } catch (e) {
    const tail = readFileSync(logPath, "utf8").split("\n").slice(-30).join("\n");
    await stop();
    throw new Error(`${e instanceof Error ? e.message : e}\nserver log:\n${tail}`);
  }
  return stop;
}

async function waitForHealth(exited: () => boolean) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (exited()) throw new Error("the server exited during startup");
    try {
      if ((await fetch(`${ADMIN_URL}/api/v1/health`)).ok) return;
    } catch {
      // Not listening yet.
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error("the server did not become healthy within 30 s");
}

async function admin(method: string, path: string, body?: BodyInit, contentType = "application/json") {
  const res = await fetch(`${ADMIN_URL}/api/v1${path}`, {
    method,
    body,
    headers: adminHeaders(body === undefined ? {} : { "content-type": contentType }),
  });
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${await res.text()}`);
}

async function put(bucket: string, key: string, body: string | Uint8Array, contentType: string) {
  await admin("PUT", `/buckets/${bucket}/object?key=${encodeURIComponent(key)}`, body as BodyInit, contentType);
}

async function seed() {
  for (const name of ["reports", "uploads"]) {
    await admin("POST", "/buckets", JSON.stringify({ name }));
  }
  await put("reports", "README.txt", "Quarterly reports.\n", "text/plain");
  await put("reports", "2025/annual.csv", "quarter,revenue\nq4,100\n", "text/csv");
  await put("reports", "2026/summary.csv", "quarter,revenue\nq3,120\n", "text/csv");
  // Larger than the inline threshold, so it is stored in blocks.
  await put("reports", "2026/q3.pdf", new Uint8Array(300_000).fill(37), "application/pdf");
  await admin(
    "POST",
    "/keys",
    JSON.stringify({ name: "billing service", buckets: { reports: { read: true } } }),
  );
}
