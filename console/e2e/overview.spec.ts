import { expect, signIn, test } from "./fixtures";

test.beforeEach(async ({ page }) => {
  await signIn(page);
  await page.goto("/");
});

test("shows storage, activity and health", async ({ page }) => {
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  for (const kpi of ["Data stored", "Objects", "Requests now", "Error rate"]) {
    await expect(page.getByText(kpi, { exact: true }).first()).toBeVisible();
  }
  await expect(page.getByText("Largest buckets")).toBeVisible();
  // The seeded bucket with the most data comes first.
  await expect(page.getByText("reports").first()).toBeVisible();

  await page.getByRole("button", { name: "1h", exact: true }).click();
  await expect(page.getByRole("button", { name: "1h", exact: true })).toHaveAttribute("aria-pressed", "true");

  // Every chart has a table view.
  await page.getByRole("button", { name: "View as table" }).first().click();
  await expect(page.getByRole("table").first()).toBeVisible();
});

test("starts a scrub", async ({ page }) => {
  await page.getByRole("button", { name: "Scrub now" }).click();
  await expect(page.getByText("Scrub started.")).toBeVisible();
});

test.describe("on a phone", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("navigates with the drawer and never scrolls sideways", async ({ page }) => {
    await page.getByRole("button", { name: "Open navigation" }).click();
    await page.getByRole("link", { name: /^Buckets/ }).click();
    await expect(page).toHaveURL("/buckets/");
    await expect(page.getByRole("heading", { name: "Buckets" })).toBeVisible();
    for (const path of ["/", "/buckets/", "/objects/?bucket=reports", "/keys/"]) {
      await page.goto(path);
      await page.waitForLoadState("networkidle");
      expect(await page.evaluate(() => document.documentElement.scrollWidth), path).toBeLessThanOrEqual(390);
    }
  });
});
