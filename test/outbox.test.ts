import { describe, test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import nodePath from "node:path";
import { mkdtempSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { SqliteOutbox } from "../src/outbox.ts";
import type { Message } from "../src/types.ts";

// Every test gets its own database file in a fresh temp directory, removed
// when the test ends. A real file, not :memory:, because several tests close
// and reopen to prove what reached disk.
function makeTempDbPath(t: TestContext): string {
  const dir = mkdtempSync(nodePath.join(tmpdir(), "outbox-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return nodePath.join(dir, "outbox.sqlite");
}

// Batch ids that sort in the order they were handed out, so tests can
// assert exact ids and exact ordering without depending on the clock.
function sequentialIds(): () => string {
  let n = 0;
  return () => `batch-${String(++n).padStart(3, "0")}`;
}

function makeOutbox(path: string, idGen: () => string = sequentialIds()): SqliteOutbox {
  return new SqliteOutbox(path, idGen);
}

function msg(ts: number, name = "one", value = ts / 10): Message {
  return { name, ts, value };
}

describe("SqliteOutbox: durability", () => {
  test("runs in WAL mode with synchronous=FULL", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());

    assert.equal(outbox.pragma("journal_mode"), "wal");
    assert.equal(outbox.pragma("synchronous"), 2); // 2 = FULL
  });

  test("once append returns, a second connection can read the rows", (t) => {
    const path = makeTempDbPath(t);
    const outbox = makeOutbox(path);
    t.after(() => outbox.close());

    outbox.append([msg(1), msg(2), msg(3)]);

    // A separate connection, as another process or a post-crash restart
    // would open. If append returned before committing, this sees nothing.
    const reader = new DatabaseSync(path);
    t.after(() => reader.close());
    const row = reader.prepare("SELECT COUNT(*) AS n FROM outbox").get() as { n: number };
    assert.equal(row.n, 3);
  });

  test("messages come back exactly as written", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    const written = [
      { name: "one", ts: 1_790_241_303_412, value: -0.4063656411097588 },
      { name: "two", ts: 1_790_241_303_412, value: 0 },
      { name: "three", ts: 1_790_241_304_412, value: 1e-9 },
    ];

    outbox.append(written);
    outbox.claim(10);

    assert.deepEqual(outbox.unpublishedBatches(1)[0]?.messages, written);
  });
});

describe("SqliteOutbox: claim", () => {
  test("claim(n) stamps the n oldest unclaimed rows and returns the id", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([1, 2, 3, 4, 5, 6, 7].map((ts) => msg(ts)));

    const id = outbox.claim(5);

    assert.equal(id, "batch-001");
    const [batch] = outbox.unpublishedBatches(10);
    assert.deepEqual(
      batch?.messages.map((m) => m.ts),
      [1, 2, 3, 4, 5],
    );
  });

  test("a second claim takes the next rows, never the already claimed ones", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([1, 2, 3, 4, 5, 6, 7].map((ts) => msg(ts)));

    outbox.claim(5);
    const second = outbox.claim(5);
    const third = outbox.claim(5);

    assert.equal(second, "batch-002");
    assert.equal(third, undefined);
    const batches = outbox.unpublishedBatches(10);
    assert.deepEqual(
      batches.map((b) => [b.batch_id, b.messages.map((m) => m.ts)]),
      [
        ["batch-001", [1, 2, 3, 4, 5]],
        ["batch-002", [6, 7]],
      ],
    );
  });

  test("claim returns undefined when there is nothing to claim", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());

    assert.equal(outbox.claim(5), undefined);
  });

  test("batch ids are unique and sort in claim order", (t) => {
    // Real UUIDv7 here, since that property is the reason for choosing it.
    const outbox = new SqliteOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([1, 2, 3].map((ts) => msg(ts)));

    const ids = [outbox.claim(1), outbox.claim(1), outbox.claim(1)];

    assert.equal(new Set(ids).size, 3);
    assert.deepEqual(
      outbox.unpublishedBatches(10).map((b) => b.messages[0]?.ts),
      [1, 2, 3],
    );
  });
});

describe("SqliteOutbox: unpublishedBatches", () => {
  test("returns at most `limit` batches, oldest first, and no unclaimed rows", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([1, 2, 3, 4, 5, 6].map((ts) => msg(ts)));
    outbox.claim(2);
    outbox.claim(2);
    outbox.claim(2);
    outbox.append([msg(7)]); // unclaimed

    const batches = outbox.unpublishedBatches(2);

    assert.deepEqual(
      batches.map((b) => b.batch_id),
      ["batch-001", "batch-002"],
    );
    const everyTs = outbox.unpublishedBatches(10).flatMap((b) => b.messages.map((m) => m.ts));
    assert.ok(!everyTs.includes(7));
  });

  test("returns an empty array when nothing is claimed", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([msg(1)]);

    assert.deepEqual(outbox.unpublishedBatches(10), []);
  });
});

describe("SqliteOutbox: message identity", () => {
  test("the same name and ts appended twice is stored once", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());

    outbox.append([msg(1), msg(1)]); // twice in one call
    outbox.append([msg(1)]); // and again in a later call
    outbox.claim(10);

    assert.equal(outbox.unpublishedBatches(1)[0]?.messages.length, 1);
  });

  test("the same ts under different names are different messages", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());

    outbox.append([msg(1, "one"), msg(1, "two")]);
    outbox.claim(10);

    assert.equal(outbox.unpublishedBatches(1)[0]?.messages.length, 2);
  });
});

describe("SqliteOutbox: ack", () => {
  test("ack(batchId) deletes that batch's rows and no others", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([1, 2, 3, 4].map((ts) => msg(ts)));
    const first = outbox.claim(2)!;
    outbox.claim(2);

    outbox.ack(first);

    const batches = outbox.unpublishedBatches(10);
    assert.deepEqual(
      batches.map((b) => [b.batch_id, b.messages.map((m) => m.ts)]),
      [["batch-002", [3, 4]]],
    );
    assert.equal(outbox.claim(10), undefined); // the deleted rows did not become unclaimed
  });

  test("ack of an unknown batch id does nothing and does not throw", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));
    t.after(() => outbox.close());
    outbox.append([msg(1)]);
    outbox.claim(10);

    assert.doesNotThrow(() => outbox.ack("never-existed"));
    assert.equal(outbox.unpublishedBatches(10).length, 1);
  });
});

describe("SqliteOutbox: close", () => {
  test("closing twice does not throw", (t) => {
    const outbox = makeOutbox(makeTempDbPath(t));

    outbox.close();
    assert.doesNotThrow(() => outbox.close());
  });
});

describe("SqliteOutbox: restart", () => {
  test("a claimed, unpublished batch survives close and reopen with the same id and messages", (t) => {
    const path = makeTempDbPath(t);
    const before = makeOutbox(path);
    before.append([msg(1), msg(2)]);
    const id = before.claim(10);
    before.close();

    const after = makeOutbox(path);
    t.after(() => after.close());

    assert.deepEqual(after.unpublishedBatches(10), [
      { batch_id: id, messages: [msg(1), msg(2)] },
    ]);
  });

  test("unclaimed rows survive close and reopen and can be claimed afterwards", (t) => {
    const path = makeTempDbPath(t);
    const before = makeOutbox(path);
    before.append([msg(1), msg(2)]);
    before.close();

    const after = makeOutbox(path);
    t.after(() => after.close());

    assert.notEqual(after.claim(10), undefined);
    assert.equal(after.unpublishedBatches(1)[0]?.messages.length, 2);
  });
});
