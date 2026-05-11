import { expect, test } from "@playwright/test";

test("console loads and tree selection drives the page inspector", async ({ page }) => {
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "B+Tree Engine Console" })).toBeVisible();
  await expect(page.locator(".panel-title", { hasText: "Tree Structure" })).toBeVisible();
  await expect(page.locator(".panel-title", { hasText: "Page Inspector" })).toBeVisible();
  await expect(page.locator("#panel-tree svg")).toBeVisible();

  await page.locator("#panel-page input[type='number']").fill("999");
  await page.getByRole("button", { name: "Inspect" }).click();
  await expect(page.locator("#panel-page")).toContainText("Error:");

  await page.locator("#panel-tree .node").first().click();
  await expect(page.locator("#panel-page")).toContainText("Page 1");
  await expect(page.locator("#panel-page")).not.toContainText("Error:");
});
