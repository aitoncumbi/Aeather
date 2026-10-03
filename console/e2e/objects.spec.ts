import { readFileSync } from "node:fs";
import { expect, signIn, test, unique } from "./fixtures";

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

test("refuses uploads past the bucket's quota before sending them", async ({ page }) => {
  const name = unique("capped");
  const headers = { "x-aether-console": "1" };
  expect((await page.request.post("/api/v1/buckets", { headers, data: { name } })).status()).toBe(201);
  const quota = await page.request.put(`/api/v1/buckets/${name}/quota`, { headers, data: { max_bytes: 5_000_000 } });
  expect(quota.status()).toBe(200);
  const sent: string[] = [];
  page.on("requestfinished", async (req) => {
    const timing = req.timing();
    sent.push(`${req.method()} ${new URL(req.url()).pathname} ${Math.round(timing.responseEnd)}ms`);
  });

  await page.goto(`/objects/?bucket=${name}`);
  // Playwright passes at most 50 MB per call, so one file at a time.
  const input = page.locator('input[type="file"]');
  const message = page.getByText(/quota of 5000000 bytes would be exceeded/);
  await input.setInputFiles({ name: "parts.bin", mimeType: "application/octet-stream", buffer: Buffer.alloc(40 * 1024 * 1024, 2) });
  await expect(message).toHaveCount(1);
  await input.setInputFiles({ name: "single.bin", mimeType: "application/octet-stream", buffer: Buffer.alloc(20 * 1024 * 1024, 1) });
  await expect(message).toHaveCount(2);
  console.log(sent.filter((r) => r.includes("/uploads") || r.includes("/object")).join("\n"));

  // The multipart upload stopped at its first request: no part was sent.
  expect(sent.some((r) => r.includes("/parts/"))).toBe(false);
  const bucket = await (await page.request.get(`/api/v1/buckets/${name}`, { headers })).json();
  expect(bucket.objects).toBe(0);
});

test("browses, restores and deletes versions in a versioned bucket", async ({ page }) => {
  const name = unique("versioned");
  const headers = { "x-aether-console": "1" };
  expect((await page.request.post("/api/v1/buckets", { headers, data: { name } })).status()).toBe(201);
  expect((await page.request.put(`/api/v1/buckets/${name}/versioning`, { headers, data: { status: "Enabled" } })).status()).toBe(200);
  for (const body of ["first draft", "second draft"]) {
    const put = await page.request.put(`/api/v1/buckets/${name}/object?key=notes.txt`, { headers, data: body });
    expect(put.status()).toBe(201);
  }

  await page.goto(`/objects/?bucket=${name}&key=notes.txt`);
  const versions = page.getByRole("list", { name: "Versions" }).getByRole("listitem");
  await expect(versions).toHaveCount(2);
  await expect(versions.first()).toContainText("current");

  // Deleting keeps the versions: the key moves to "Deleted objects".
  await page.getByRole("button", { name: "Delete object" }).click();
  await expect(page.getByRole("dialog")).toContainText("Its versions are kept");
  await page.getByRole("dialog").getByRole("button", { name: "Delete object" }).click();
  await expect(page.getByRole("button", { name: /^notes\.txt/ })).toHaveCount(0);
  await page.getByRole("button", { name: "Deleted objects" }).click();
  await page.getByRole("button", { name: /^notes\.txt/ }).click();
  await expect(page.getByText("Deleted. Restore a version to bring it back.")).toBeVisible();
  await expect(versions).toHaveCount(3);
  await expect(versions.first()).toContainText("delete marker");

  // Restore the first draft: it becomes the current version again.
  await versions.last().getByRole("button", { name: "Restore" }).click();
  await expect(page.getByText(/^Restored the version/)).toBeVisible();
  await expect(versions).toHaveCount(4);
  const current = await page.request.get(`/api/v1/buckets/${name}/object/download?key=notes.txt`, { headers });
  expect(await current.text()).toBe("first draft");

  // Delete the oldest version for good.
  await versions.last().getByRole("button", { name: /permanently$/ }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete version" }).click();
  await expect(versions).toHaveCount(3);
});
