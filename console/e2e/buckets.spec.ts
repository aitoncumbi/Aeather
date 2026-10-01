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
