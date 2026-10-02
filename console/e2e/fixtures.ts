import { test as base, expect, type Page } from "@playwright/test";
import { ROOT } from "./server";

export { expect };

/** Signs the page's browser context in as root through the login endpoint. */
export async function signIn(page: Page) {
  const res = await page.request.post("/api/v1/login", {
    headers: { "x-aether-console": "1" },
    data: { access_key_id: ROOT.id, secret_key: ROOT.secret },
  });
  expect(res.status()).toBe(200);
}

/** A name no earlier test (or run against the same server) has used. */
export function unique(prefix: string) {
  return `${prefix}-${Date.now().toString(36)}`;
}

// Every test fails on an uncaught exception or a console error in the page.
// Failed requests are left to the assertions: the console expects some 401s,
// such as the session check before signing in.
export const test = base.extend<{ pageErrors: void }>({
  pageErrors: [
    async ({ page }, use) => {
      const errors: string[] = [];
      page.on("pageerror", (e) => errors.push(e.message));
      page.on("console", (m) => {
        if (m.type() === "error" && !m.text().startsWith("Failed to load resource")) errors.push(m.text());
      });
      await use();
      expect(errors, "errors in the page").toEqual([]);
    },
    { auto: true },
  ],
});
