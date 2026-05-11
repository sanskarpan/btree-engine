import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  use: {
    baseURL: "http://127.0.0.1:3001",
    browserName: "chromium",
    headless: true,
  },
  webServer: [
    {
      command: "bun run scripts/run-backend-smoke.ts",
      cwd: ".",
      url: "http://127.0.0.1:18080/health/live",
      timeout: 120_000,
      reuseExistingServer: !process.env.CI,
      env: {
        TEST_BACKEND_PORT: "18080",
        TEST_API_KEY: "test-key",
      },
    },
    {
      command: "bun run dev",
      cwd: ".",
      url: "http://127.0.0.1:3001/health",
      timeout: 120_000,
      reuseExistingServer: !process.env.CI,
      env: {
        BFF_PORT: "3001",
        BACKEND_URL: "http://127.0.0.1:18080",
        BACKEND_API_KEY: "test-key",
      },
    },
  ],
});
