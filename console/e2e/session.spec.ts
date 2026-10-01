import { ROOT } from "./server";
import { expect, test } from "./fixtures";

test("signing in returns to the page that was asked for, and signing out ends the session", async ({ page }) => {
  await page.goto("/keys/");
  await expect(page).toHaveURL(/\/login\/\?next=%2Fkeys%2F$/);

  await page.getByLabel("Access key ID").fill(ROOT.id);
  await page.getByLabel("Secret key", { exact: true }).fill("not-the-secret");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.locator(".error-text")).toContainText("secret key is wrong");

  await page.getByLabel("Secret key", { exact: true }).fill(ROOT.secret);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page).toHaveURL("/keys/");
  await expect(page.getByRole("heading", { name: "Access keys" })).toBeVisible();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL(/\/login\//);
  const session = await page.request.get("/api/v1/session", { headers: { "x-aether-console": "1" } });
  expect(session.status()).toBe(401);
  // No Basic challenge, so browsers never show their own sign-in dialog.
  expect(session.headers()["www-authenticate"]).toBeUndefined();
});

for (const next of ["//example.com/", "/\\example.com/", "https://example.com/"]) {
  test(`a sign-in link cannot redirect to another site: ${next}`, async ({ page }) => {
    await page.goto(`/login/?next=${encodeURIComponent(next)}`);
    await page.getByLabel("Access key ID").fill(ROOT.id);
    await page.getByLabel("Secret key", { exact: true }).fill(ROOT.secret);
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page).toHaveURL("/");
  });
}

test("writes with the session cookie need the console header", async ({ page }) => {
  await page.request.post("/api/v1/login", {
    headers: { "x-aether-console": "1" },
    data: { access_key_id: ROOT.id, secret_key: ROOT.secret },
  });
  const forged = await page.request.post("/api/v1/buckets", { data: { name: "csrf-attempt" } });
  expect(forged.status()).toBe(403);
});
