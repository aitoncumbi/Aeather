import { adminHeaders } from "./server";
import { expect, signIn, test, unique } from "./fixtures";

test.beforeEach(async ({ page }) => {
  await signIn(page);
  await page.goto("/keys/");
});

test("changes a key's bucket permissions", async ({ page, request }) => {
  await page.getByRole("button", { name: /^billing service/ }).click();
  const write = page.getByRole("checkbox", { name: "Write on reports" });
  await expect(write).not.toBeChecked();
  await write.check();
  await expect(write).toBeChecked();

  await expect
    .poll(async () => {
      const keys: { name: string; buckets: Record<string, { write: boolean }> }[] = await (
        await request.get("/api/v1/keys", { headers: adminHeaders() })
      ).json();
      return keys.find((k) => k.name === "billing service")?.buckets.reports?.write;
    })
    .toBe(true);
});

test("creates a key and shows its secret once", async ({ page }) => {
  const name = unique("media-api");
  await page.getByRole("button", { name: "Create key" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("Name").fill(name);
  await page.locator("#ck-add").selectOption("uploads");
  await dialog.getByRole("button", { name: "Add", exact: true }).click();
  await dialog.getByRole("button", { name: "Write on uploads" }).click();
  await dialog.getByRole("button", { name: "Create key" }).click();

  await expect(dialog.getByText("Key created")).toBeVisible();
  await expect(dialog.getByText(/AWS_SECRET_ACCESS_KEY=/)).toBeVisible();
  const done = dialog.getByRole("button", { name: "Done" });
  await expect(done).toBeDisabled();
  await dialog.getByLabel("I have saved the secret key somewhere safe").check();
  await done.click();

  await expect(page.getByRole("heading", { name })).toBeVisible();
  await expect(page.getByRole("checkbox", { name: "Write on uploads" })).toBeChecked();
});
