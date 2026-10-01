// Where the end-to-end tests find the server that global-setup.ts starts.

export const S3_PORT = Number(process.env.E2E_S3_PORT ?? 19600);
export const ADMIN_PORT = Number(process.env.E2E_ADMIN_PORT ?? 19601);
export const ADMIN_URL = `http://127.0.0.1:${ADMIN_PORT}`;

export const ROOT = { id: "e2e-root", secret: "e2e-root-secret-key-0123456789" };

/** Admin API requests from tests and setup, authenticated as root. */
export function adminHeaders(extra: Record<string, string> = {}): Record<string, string> {
  return { authorization: `Basic ${Buffer.from(`${ROOT.id}:${ROOT.secret}`).toString("base64")}`, ...extra };
}
