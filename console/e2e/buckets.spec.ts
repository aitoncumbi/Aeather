import { expect, signIn, test, unique } from "./fixtures";

test.beforeEach(async ({ page }) => {
  await signIn(page);
});

test("creates and deletes a bucket", async ({ page }) => {
  const name = unique("photos");
  await page.goto("/buckets/");
  await page.getByRole("button", { name: "Create bucket" }).click();
  await page.getByRole("dialog").getByLabel("Name").fill("Not_Valid");
  await expect(page.getByRole("dialog").getByRole("button", { name: "Create bucket" })).toBeDisabled();
  await page.getByRole("dialog").getByLabel("Name").fill(name);
  await page.getByRole("dialog").getByRole("button", { name: "Create bucket" }).click();
  await expect(page.getByText(`Created bucket ${name}.`)).toBeVisible();

  await page.getByRole("button", { name: new RegExp(`^${name}`) }).click();
  await expect(page.getByText("The bucket is empty and can be deleted.")).toBeVisible();
  await page.getByRole("button", { name: "Delete", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "Delete bucket" }).click();
  await expect(page.getByText(`Deleted bucket ${name}.`)).toBeVisible();
  await expect(page.getByRole("button", { name: new RegExp(`^${name}`) })).toHaveCount(0);
});

test("shows who can reach a bucket and refuses to delete one with objects", async ({ page }) => {
  await page.goto("/buckets/");
  await page.getByRole("button", { name: /^reports/ }).click();
  await expect(page.getByText("billing service")).toBeVisible();
  await expect(page.getByText(/Only empty buckets can be deleted/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Delete", exact: true })).toBeDisabled();
});

test("sets and clears a bucket quota", async ({ page }) => {
  const name = unique("quota");
  const res = await page.request.post("/api/v1/buckets", { headers: { "x-aether-console": "1" }, data: { name } });
  expect(res.status()).toBe(201);
  await page.goto(`/buckets/?bucket=${name}`);
  await page.getByRole("button", { name: new RegExp(`^${name}`) }).click();
  await expect(page.getByText("No limit")).toBeVisible();

  await page.getByRole("button", { name: "Edit" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Largest total size, in GB").fill("-1");
  await expect(dialog.getByRole("button", { name: "Save quota" })).toBeDisabled();
  await dialog.getByLabel("Largest total size, in GB").fill("2.5");
  await dialog.getByLabel("Most objects").fill("1000");
  await dialog.getByRole("button", { name: "Save quota" }).click();
  await expect(page.getByText(`Updated the quota of ${name}.`)).toBeVisible();
  await expect(page.getByText("0 B of 2.50 GB · 0 of 1,000 objects")).toBeVisible();

  await page.getByRole("button", { name: "Edit" }).click();
  await expect(dialog.getByLabel("Largest total size, in GB")).toHaveValue("2.5");
  await dialog.getByLabel("Largest total size, in GB").fill("");
  await dialog.getByLabel("Most objects").fill("");
  await dialog.getByRole("button", { name: "Save quota" }).click();
  await expect(page.getByText("No limit")).toBeVisible();
});
