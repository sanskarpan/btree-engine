import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const port = Number(process.env.TEST_BACKEND_PORT ?? "18080");
const apiKey = process.env.TEST_API_KEY ?? "test-key";
const repoRoot = resolve(import.meta.dir, "..", "..");

const tmpRoot = mkdtempSync(join(tmpdir(), "btree-frontend-smoke-"));
const dataDir = join(tmpRoot, "data");
const walDir = join(tmpRoot, "wal");
mkdirSync(dataDir, { recursive: true });
mkdirSync(walDir, { recursive: true });

const configPath = join(tmpRoot, "config.yaml");
writeFileSync(
  configPath,
  [
    "engine:",
    `  data_file: ${join(dataDir, "btree.db")}`,
    "  buffer_pool_size: 64",
    "wal:",
    `  log_file: ${join(walDir, "wal.log")}`,
    "  buffer_size: 65536",
    "  sync_on_commit: true",
    "mvcc:",
    "  default_isolation: snapshot",
    "gateway:",
    `  port: ${port}`,
    `  api_keys: [${JSON.stringify(apiKey)}]`,
    "log_level: info",
    "log_format: text",
    "",
  ].join("\n")
);

const proc = Bun.spawn(["go", "run", "./cmd/server", "--config", configPath], {
  cwd: repoRoot,
  stdout: "inherit",
  stderr: "inherit",
});

const cleanup = () => {
  proc.kill();
  rmSync(tmpRoot, { recursive: true, force: true });
};

process.on("SIGINT", () => {
  cleanup();
  process.exit(130);
});

process.on("SIGTERM", () => {
  cleanup();
  process.exit(143);
});

const exitCode = await proc.exited;
rmSync(tmpRoot, { recursive: true, force: true });
process.exit(exitCode);
