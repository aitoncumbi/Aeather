import { defineConfig, devices } from "@playwright/test";
import { ADMIN_URL } from "./e2e/server";

// End-to-end tests drive the console embedded in a release build of the
// server; e2e/global-setup.ts starts that server with some data.
export default defineConfig({
  testDir: "e2e",
  globalSetup: "./e2e/global-setup.ts",
  // The tests share one server, so they run one at a time.
  workers: 1,
  forbidOnly: !!process.env.CI,
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL: ADMIN_URL,
    trace: "retain-on-failure",
    // For a preinstalled Chromium other than the one this Playwright expects.
    launchOptions: { executablePath: process.env.CHROMIUM_PATH || undefined },
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } }],
});
