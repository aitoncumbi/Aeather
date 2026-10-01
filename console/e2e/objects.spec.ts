import { readFileSync } from "node:fs";
import { expect, signIn, test } from "./fixtures";

test.beforeEach(async ({ page }) => {
  await signIn(page);
});

test("browses prefixes and shares a presigned link", async ({ page }) => {
  await page.goto("/objects/?bucket=reports");
  await expect(page.getByRole("button", { name: /^README\.txt/ })).toBeVisible();
  await page.getByRole("button", { name: /^2026\// }).click();
  await expect(page).toHaveURL(/prefix=2026%2F/);

  await page.getByRole("button", { name: /^q3\.pdf/ }).click();
  await expect(page.getByText("application/pdf").first()).toBeVisible();
  await page.getByRole("button", { name: "Copy link" }).click();
  const url = await page.locator(".card code").textContent();
  expect(url).toMatch(/X-Amz-Signature=/);

  // The link is an ordinary S3 URL: anyone can fetch it, until it expires.
  const shared = await page.request.get(url!, { headers: { cookie: "" } });
  expect(shared.status()).toBe(200);
  expect((await shared.body()).length).toBe(300_000);
});

test("uploads small and multipart files, downloads and deletes them", async ({ page }) => {
  test.slow();
  await page.goto("/objects/?bucket=uploads");
  await expect(page.getByText("Nothing here yet")).toBeVisible();

  // Over 32 MiB, so the console uploads it in parts.
  const big = Buffer.alloc(40 * 1024 * 1024, 7);
  await page.locator('input[type="file"]').setInputFiles([
    { name: "hello.txt", mimeType: "text/plain", buffer: Buffer.from("hello from the console\n") },
    { name: "big.bin", mimeType: "application/octet-stream", buffer: big },
  ]);
  await expect(page.getByRole("button", { name: /^hello\.txt/ })).toBeVisible();
  await expect(page.getByRole("button", { name: /^big\.bin/ })).toBeVisible({ timeout: 60_000 });

  await page.getByRole("button", { name: /^big\.bin/ }).click();
  await expect(page.getByText(/multipart, 3 parts/)).toBeVisible();
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    page.getByRole("link", { name: "Download" }).click(),
  ]);
  expect(download.suggestedFilename()).toBe("big.bin");
  expect(readFileSync((await download.path())!).length).toBe(big.length);

  await page.getByRole("button", { name: /^hello\.txt/ }).click();
  await page.getByRole("button", { name: "Delete object" }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete object" }).click();
  await expect(page.getByText("Deleted hello.txt.")).toBeVisible();
  await expect(page.getByRole("button", { name: /^hello\.txt/ })).toHaveCount(0);
});
