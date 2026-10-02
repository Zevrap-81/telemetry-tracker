// Proves the toolchain and the wiring: TypeScript runs directly, node:test
// works, node:sqlite is available, and the real process starts, produces
// batches on stdout, and shuts down cleanly on SIGINT.

import { describe, test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { sources } from "../src/sources.ts";
import type { Batch } from "../src/types.ts";

test("sources are deterministic in time", () => {
  assert.equal(sources["sin"]!(0), 0);
  assert.equal(sources["cos"]!(0), 1);
});

test("node:sqlite is available", () => {
  const db = new DatabaseSync(":memory:");
  const row = db.prepare("select 1 + 1 as two").get() as { two: number };
  assert.equal(row.two, 2);
  db.close();
});

describe("the application end to end", () => {
  const trackers = [
    { name: "one", source: "sin", sampleMs: 1000, precision: 0.08 },
    { name: "two", source: "cos", sampleMs: 1000, precision: 0.15 },
    { name: "three", source: "sin", sampleMs: 1000, precision: 5 },
  ];

  // The one test that waits on real time, because the subject is the real
  // process. About two seconds: enough for the first samples at 100 ms to be
  // claimed and published by the 500 ms forwarder tick, several times over.
  const RUN_MS = 2_000;

  async function runApp(t: TestContext) {
    const dir = mkdtempSync(join(tmpdir(), "smoke-"));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const configPath = join(dir, "config.json");
    const outboxPath = join(dir, "outbox.sqlite");
    writeFileSync(configPath, JSON.stringify(trackers));

    const child = spawn(process.execPath, ["src/main.ts"], {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        CONFIG_PATH: configPath,
        OUTBOX_PATH: outboxPath,
        TICK_MS: "100",
        FORWARD_INTERVAL_MS: "500",
        PUBLISH_FAILURE_RATE: "0",
      },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d: Buffer) => (stdout += d.toString()));
    child.stderr.on("data", (d: Buffer) => (stderr += d.toString()));

    await new Promise((r) => setTimeout(r, RUN_MS));
    child.kill("SIGINT");
    const killer = setTimeout(() => child.kill("SIGKILL"), 10_000); // safety net, must not be needed
    const [code, signal] = (await once(child, "exit")) as [number | null, string | null];
    clearTimeout(killer);

    return { code, signal, stdout, stderr, outboxPath };
  }

  test("starts, publishes batches on stdout only, and exits cleanly on SIGINT", async (t) => {
    const { code, signal, stdout, stderr, outboxPath } = await runApp(t);

    assert.equal(signal, null, `killed by ${signal}, stderr:\n${stderr}`);
    assert.equal(code, 0, `exit code ${code}, stderr:\n${stderr}`);

    // stdout: only complete JSON lines, each a Batch
    const lines = stdout.split("\n").filter((l) => l !== "");
    assert.ok(lines.length >= 1, `no batches on stdout, stderr:\n${stderr}`);
    const batches = lines.map((l) => JSON.parse(l) as Batch);
    for (const b of batches) {
      assert.equal(typeof b.batch_id, "string");
      assert.ok(Array.isArray(b.messages) && b.messages.length > 0);
      for (const m of b.messages) {
        assert.ok(trackers.some((tr) => tr.name === m.name), `unknown tracker ${m.name}`);
        assert.equal(typeof m.ts, "number");
        assert.equal(typeof m.value, "number");
      }
    }

    // The first tick samples every tracker, and every first sample is kept.
    assert.deepEqual(
      batches[0]!.messages.map((m) => m.name).sort(),
      trackers.map((tr) => tr.name).sort(),
    );

    // Every message appears under exactly one batch id, even before dedup,
    // since nothing failed in this run.
    const seen = new Map<string, string>();
    for (const b of batches) {
      for (const m of b.messages) {
        const key = `${m.name}@${m.ts}`;
        assert.equal(seen.get(key), undefined, `${key} appeared in two batches`);
        seen.set(key, b.batch_id);
      }
    }

    // stderr: logs, never batches
    for (const line of stderr.split("\n")) {
      assert.ok(!line.startsWith('{"batch_id"'), "a batch leaked to stderr");
    }
    assert.match(stderr, /journal_mode=wal synchronous=2/);
    assert.match(stderr, /SIGINT received/);

    // The outbox after exit: every claimed batch was also written to stdout,
    // because claim and publish happen in the same synchronous tick. Rows
    // sampled after the last tick may remain, unclaimed, for the next start.
    const db = new DatabaseSync(outboxPath, { readOnly: true });
    t.after(() => db.close());
    const claimed = db
      .prepare("SELECT DISTINCT batchId AS id FROM outbox WHERE batchId IS NOT NULL")
      .all() as { id: string }[];
    const published = new Set(batches.map((b) => b.batch_id));
    for (const { id } of claimed) {
      assert.ok(published.has(id), `batch ${id} claimed but never written to stdout`);
    }
  });

  test("refuses to start on an invalid config", async (t) => {
    const dir = mkdtempSync(join(tmpdir(), "smoke-"));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const configPath = join(dir, "config.json");
    writeFileSync(configPath, JSON.stringify([{ name: "bad", source: "tan", sampleMs: 10, precision: 1 }]));

    const child = spawn(process.execPath, ["src/main.ts"], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, CONFIG_PATH: configPath, OUTBOX_PATH: join(dir, "o.sqlite") },
    });
    let stderr = "";
    child.stderr.on("data", (d: Buffer) => (stderr += d.toString()));
    const [code] = (await once(child, "exit")) as [number | null];

    assert.notEqual(code, 0);
    assert.match(stderr, /unknown source "tan"/);
    assert.match(stderr, /sampleMs/);
  });
});
