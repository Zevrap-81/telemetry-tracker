// The crash test: the brief's three requirements, checked end to end.
//
// Run the real process, kill it with SIGKILL at random moments (a power cut
// from the process's point of view: no handler, no cleanup, mid-write if that
// is where it lands), restart, repeat. Then drain with failures off and check
// everything that ever reached stdout:
//
//   no loss        every message the outbox ever held appears on stdout
//   no duplicates  every name+ts appears under exactly one batch_id
//   crash safety   PRAGMA integrity_check is "ok" after every kill
//
// Takes about 15 s of real time, so it has its own script: npm run test:crash

import { describe, test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { Batch } from "../src/types.ts";

const KILLS = 5;
const TRACKERS = Array.from({ length: 50 }, (_, i) => ({
  name: `t${i}`,
  source: i % 2 ? "sin" : "cos",
  sampleMs: 1000,
  precision: 0.001, // keep nearly every sample so there is plenty to lose
}));

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const key = (m: { name: string; ts: number }) => `${m.name}@${m.ts}`;

interface App {
  child: ChildProcess;
  stdout: () => string;
  stderr: () => string;
}

function startApp(configPath: string, outboxPath: string, failureRate: number): App {
  let out = "";
  let err = "";
  const child = spawn(process.execPath, ["src/main.ts"], {
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      CONFIG_PATH: configPath,
      OUTBOX_PATH: outboxPath,
      TICK_MS: "100",
      FORWARD_INTERVAL_MS: "500",
      PUBLISH_TIMEOUT_MS: "1500",
      PUBLISH_FAILURE_RATE: String(failureRate),
    },
  });
  child.stdout!.on("data", (d: Buffer) => (out += d.toString()));
  child.stderr!.on("data", (d: Buffer) => (err += d.toString()));
  return { child, stdout: () => out, stderr: () => err };
}

function withDb<T>(path: string, fn: (db: DatabaseSync) => T): T {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    return fn(db);
  } finally {
    db.close();
  }
}

describe("crash test", () => {
  test(`survives ${KILLS} SIGKILLs with no loss and no duplicates`, async (t: TestContext) => {
    const dir = mkdtempSync(join(tmpdir(), "crash-"));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const configPath = join(dir, "config.json");
    const outboxPath = join(dir, "outbox.sqlite");
    writeFileSync(configPath, JSON.stringify(TRACKERS));

    let allStdout = "";
    const committed = new Set<string>(); // every name+ts the outbox was ever seen to hold

    // 1. Run and kill, several times.
    for (let i = 0; i < KILLS; i++) {
      const app = startApp(configPath, outboxPath, 0.5);
      await sleep(600 + Math.random() * 1_600);
      app.child.kill("SIGKILL");
      await once(app.child, "exit");
      allStdout += app.stdout();

      // crash safety: the database must open and be intact after every kill
      withDb(outboxPath, (db) => {
        const check = db.prepare("PRAGMA integrity_check").get() as { integrity_check: string };
        assert.equal(check.integrity_check, "ok", `integrity_check failed after kill ${i + 1}`);
        for (const row of db.prepare("SELECT name, ts FROM outbox").all() as { name: string; ts: number }[]) {
          committed.add(key(row));
        }
      });
    }
    assert.ok(committed.size > 0, "the kills landed before anything was committed; test is not exercising anything");

    // 2. Drain with failures off until nothing claimed is left, then stop cleanly.
    const drain = startApp(configPath, outboxPath, 0);
    const deadline = Date.now() + 20_000;
    for (;;) {
      await sleep(250);
      const left = withDb(outboxPath, (db) =>
        (db.prepare("SELECT COUNT(*) AS n FROM outbox WHERE batchId IS NOT NULL").get() as { n: number }).n,
      );
      if (left === 0) break;
      assert.ok(Date.now() < deadline, `drain did not finish: ${left} claimed rows left\n${drain.stderr()}`);
    }
    drain.child.kill("SIGINT");
    const [code] = (await once(drain.child, "exit")) as [number | null];
    assert.equal(code, 0, drain.stderr());
    allStdout += drain.stdout();

    // 3. Check everything that ever reached stdout.
    const idOf = new Map<string, string>(); // message key -> the one batch_id it appeared under
    const timesSeen = new Map<string, number>(); // batch_id -> how many times it was written
    for (const line of allStdout.split("\n").filter((l) => l !== "")) {
      let batch: Batch;
      try {
        batch = JSON.parse(line) as Batch;
      } catch {
        assert.fail(`torn or interleaved line on stdout: ${line.slice(0, 80)}`);
      }
      timesSeen.set(batch.batch_id, (timesSeen.get(batch.batch_id) ?? 0) + 1);
      for (const m of batch.messages) {
        const k = key(m);
        const before = idOf.get(k);
        assert.ok(
          before === undefined || before === batch.batch_id,
          `no duplicates violated: ${k} appeared under ${before} and ${batch.batch_id}`,
        );
        idOf.set(k, batch.batch_id);
      }
    }

    // no loss: everything the outbox ever held is on stdout
    const lost = [...committed].filter((k) => !idOf.has(k));
    assert.deepEqual(lost, [], `no loss violated: ${lost.length} committed messages never reached stdout`);

    // the retry path really ran: at least one batch was written more than once
    const resent = [...timesSeen.values()].filter((n) => n > 1).length;
    assert.ok(resent > 0, "no batch was ever resent; the failure rate did not bite, nothing was tested");

    console.error(
      `crash test: ${KILLS} kills, ${committed.size} messages seen in the outbox, ` +
        `${idOf.size} unique messages on stdout, ${timesSeen.size} batches, ${resent} resent`,
    );
  });
});
