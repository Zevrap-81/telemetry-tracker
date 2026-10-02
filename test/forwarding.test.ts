// The forwarder is driven through start() and fake timers: start() forwards
// once immediately, and t.mock.timers.tick(forwardIntervalMs) runs the next
// tick. The publish timeout is also a fake timer, so a test that wants a
// batch to stay in flight across a tick sets publishTimeoutMs larger than
// forwardIntervalMs, and a test about the timeout sets it smaller.
//
// The outbox is the real SqliteOutbox on ":memory:", so claim and ack
// semantics are real and the ids are predictable through sequentialIds().

import { describe, test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { Forwarder, type ForwarderConfig, type Publisher } from "../src/forwarding.ts";
import { SqliteOutbox } from "../src/outbox.ts";
import type { Batch, Message } from "../src/types.ts";

const INTERVAL_MS = 10_000;
const TIMEOUT_MS = 5_000;

function sequentialIds(): () => string {
  let n = 0;
  return () => `batch-${String(++n).padStart(3, "0")}`;
}

function msg(ts: number): Message {
  return { name: "one", ts, value: ts / 10 };
}

/** Let queued microtasks run. setImmediate is not faked by t.mock.timers. */
const drain = () => new Promise<void>((r) => setImmediate(r));

interface Attempt {
  batch: Batch;
  resolve: () => void;
  reject: (err: Error) => void;
}

/**
 * A publisher the test controls. Every publish() is recorded as an attempt
 * with the handles to settle it later: resolve(), reject(), or never.
 */
class ScriptedPublisher implements Publisher {
  readonly attempts: Attempt[] = [];

  publish(batch: Batch): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.attempts.push({ batch, resolve, reject });
    });
  }

  /** Batch ids in the order they were published, repeats included. */
  get ids(): string[] {
    return this.attempts.map((a) => a.batch.batch_id);
  }

  attempt(i: number): Attempt {
    const a = this.attempts[i];
    assert.ok(a, `no publish attempt #${i}, only ${this.attempts.length}`);
    return a;
  }
}

/** An outbox whose next ack throws, to test the ack failure path. */
class FlakyAckOutbox extends SqliteOutbox {
  failNextAck = false;

  override ack(batchId: string): void {
    if (this.failNextAck) {
      this.failNextAck = false;
      throw new Error("disk on fire");
    }
    super.ack(batchId);
  }
}

interface Rig {
  outbox: SqliteOutbox;
  publisher: ScriptedPublisher;
  forwarder: Forwarder;
  /** Ids of batches the outbox still holds, oldest first. */
  remaining: () => string[];
}

function rig(
  t: TestContext,
  overrides: Partial<ForwarderConfig> = {},
  outbox: SqliteOutbox = new SqliteOutbox(":memory:", sequentialIds()),
): Rig {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  t.mock.method(console, "error", () => {}); // keep stderr quiet; tests assert on it where it matters
  const publisher = new ScriptedPublisher();
  const config: ForwarderConfig = {
    batchSize: 1,
    forwardIntervalMs: INTERVAL_MS,
    maxInFlight: 1,
    // Long by default so an unsettled publish stays in flight across ticks.
    publishTimeoutMs: 10 * INTERVAL_MS,
    ...overrides,
  };
  const forwarder = new Forwarder(outbox, publisher, config);
  t.after(() => {
    forwarder.stop();
    outbox.close();
  });
  return {
    outbox,
    publisher,
    forwarder,
    remaining: () => outbox.unpublishedBatches(100).map((b) => b.batch_id),
  };
}

describe("Forwarder: claim", () => {
  test("a tick claims every unclaimed row, in chunks of batchSize", (t) => {
    const r = rig(t, { batchSize: 5, maxInFlight: 10 });
    r.outbox.append(Array.from({ length: 12 }, (_, i) => msg(i + 1)));

    r.forwarder.start();

    assert.deepEqual(
      r.publisher.attempts.map((a) => a.batch.messages.length),
      [5, 5, 2],
    );
    assert.equal(r.outbox.claim(5), undefined); // nothing left unclaimed
  });

  test("a tick with nothing unclaimed claims nothing", (t) => {
    const r = rig(t);

    r.forwarder.start();

    assert.deepEqual(r.publisher.ids, []);
    assert.deepEqual(r.remaining(), []);
  });
});

describe("Forwarder: dispatch", () => {
  test("a resolved publish acks the batch and frees the slot", async (t) => {
    const r = rig(t);
    r.outbox.append([msg(1), msg(2)]);
    r.forwarder.start(); // publishes batch-001, slot full

    r.publisher.attempt(0).resolve();
    await drain();

    assert.deepEqual(r.remaining(), ["batch-002"]); // batch-001 is gone
    t.mock.timers.tick(INTERVAL_MS);
    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002"]); // slot was free
  });

  test("a rejected publish leaves the rows in place and frees the slot", async (t) => {
    const r = rig(t);
    r.outbox.append([msg(1)]);
    r.forwarder.start();

    r.publisher.attempt(0).reject(new Error("network"));
    await drain();

    assert.deepEqual(r.remaining(), ["batch-001"]);
    t.mock.timers.tick(INTERVAL_MS);
    assert.equal(r.publisher.attempts.length, 2); // slot was free, so it was retried
  });

  test("a publish that never settles times out, leaves the rows, and frees the slot", async (t) => {
    const r = rig(t, { publishTimeoutMs: TIMEOUT_MS });
    const errors = t.mock.method(console, "error", () => {});
    r.outbox.append([msg(1)]);
    r.forwarder.start();

    t.mock.timers.tick(TIMEOUT_MS);
    await drain();

    assert.deepEqual(r.remaining(), ["batch-001"]);
    assert.match(String(errors.mock.calls[0]?.arguments[0]), /timed out/);
    t.mock.timers.tick(INTERVAL_MS - TIMEOUT_MS);
    assert.equal(r.publisher.attempts.length, 2); // slot was free at the next tick
  });

  test("the same batch is republished under the same id on the next tick after a failure", async (t) => {
    const r = rig(t);
    r.outbox.append([msg(1)]);
    r.forwarder.start();

    r.publisher.attempt(0).reject(new Error("network"));
    await drain();
    t.mock.timers.tick(INTERVAL_MS);

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-001"]);
    assert.deepEqual(r.publisher.attempt(1).batch, r.publisher.attempt(0).batch);
  });

  test("a late settle after a timeout does nothing", async (t) => {
    const r = rig(t, { publishTimeoutMs: TIMEOUT_MS });
    r.outbox.append([msg(1)]);
    r.forwarder.start();
    t.mock.timers.tick(TIMEOUT_MS); // first attempt times out
    await drain(); // let the timeout's finally free the slot
    t.mock.timers.tick(INTERVAL_MS - TIMEOUT_MS); // next tick retries
    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-001"]);

    r.publisher.attempt(1).resolve(); // the retry succeeds
    await drain();
    assert.deepEqual(r.remaining(), []);

    r.publisher.attempt(0).resolve(); // the abandoned first attempt settles late
    r.publisher.attempt(0).reject(new Error("and rejects too")); // no-op, already settled
    await drain();

    assert.deepEqual(r.remaining(), []);
    t.mock.timers.tick(INTERVAL_MS);
    assert.equal(r.publisher.attempts.length, 2); // nothing new was started
  });

  test("an ack that throws is logged and the slot is still freed", async (t) => {
    const outbox = new FlakyAckOutbox(":memory:", sequentialIds());
    const r = rig(t, {}, outbox);
    const errors = t.mock.method(console, "error", () => {});
    r.outbox.append([msg(1)]);
    r.forwarder.start();

    outbox.failNextAck = true;
    r.publisher.attempt(0).resolve();
    await drain();

    assert.equal(errors.mock.callCount(), 1);
    assert.deepEqual(r.remaining(), ["batch-001"]); // still there, ack did not happen
    t.mock.timers.tick(INTERVAL_MS);
    assert.equal(r.publisher.attempts.length, 2); // slot was freed, retried
  });
});

describe("Forwarder: slots", () => {
  test("a tick starts at most maxInFlight publishes", (t) => {
    const r = rig(t, { maxInFlight: 2 });
    r.outbox.append([1, 2, 3, 4, 5].map(msg));

    r.forwarder.start();

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002"]);
  });

  test("a tick skips batches that are already in flight", (t) => {
    const r = rig(t, { maxInFlight: 2 });
    r.outbox.append([1, 2].map(msg));
    r.forwarder.start();

    t.mock.timers.tick(INTERVAL_MS); // both still hanging

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002"]); // no repeats
  });

  test("in-flight batches do not block the batches behind them", async (t) => {
    const r = rig(t, { maxInFlight: 2 });
    r.outbox.append([1, 2, 3].map(msg));
    r.forwarder.start(); // batch-001 and batch-002 in flight

    r.publisher.attempt(1).resolve(); // batch-002 done, batch-001 still hangs
    await drain();
    t.mock.timers.tick(INTERVAL_MS);

    // The oldest batch is still in flight. The free slot must go to batch-003,
    // not be wasted on fetching batch-001 again and skipping it.
    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002", "batch-003"]);
  });

  test("a freed slot is refilled on the next tick", async (t) => {
    const r = rig(t, { maxInFlight: 1 });
    r.outbox.append([1, 2].map(msg));
    r.forwarder.start();
    t.mock.timers.tick(INTERVAL_MS);
    assert.deepEqual(r.publisher.ids, ["batch-001"]); // slot still taken

    r.publisher.attempt(0).resolve();
    await drain();
    t.mock.timers.tick(INTERVAL_MS);

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002"]);
  });

  test("maxInFlight 1 publishes one batch per tick, oldest first", async (t) => {
    const r = rig(t, { maxInFlight: 1 });
    r.outbox.append([1, 2, 3].map(msg));
    r.forwarder.start();

    for (let i = 0; i < 3; i++) {
      r.publisher.attempt(i).resolve();
      await drain();
      t.mock.timers.tick(INTERVAL_MS);
    }

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002", "batch-003"]);
    assert.deepEqual(r.remaining(), []);
  });
});

describe("Forwarder: lifecycle", () => {
  test("start forwards once immediately and then every forwardIntervalMs", async (t) => {
    const r = rig(t);
    r.outbox.append([msg(1)]);

    r.forwarder.start();
    assert.equal(r.publisher.attempts.length, 1); // no wait for the first tick

    r.publisher.attempt(0).resolve();
    await drain();
    r.outbox.append([msg(2)]);
    t.mock.timers.tick(INTERVAL_MS - 1);
    assert.equal(r.publisher.attempts.length, 1); // not yet
    t.mock.timers.tick(1);
    assert.equal(r.publisher.attempts.length, 2); // on the interval
  });

  test("stop ends the interval and does not wait for in-flight publishes", (t) => {
    const r = rig(t);
    r.outbox.append([msg(1)]);
    r.forwarder.start(); // batch-001 in flight and will never settle

    r.forwarder.stop(); // returns at once
    r.outbox.append([msg(2)]);
    t.mock.timers.tick(3 * INTERVAL_MS);

    assert.deepEqual(r.publisher.ids, ["batch-001"]); // no further ticks
    assert.deepEqual(r.remaining(), ["batch-001"]); // nothing was acked or claimed
  });

  test("calling start twice does not create two intervals", (t) => {
    const r = rig(t);
    r.forwarder.start();
    r.forwarder.start();

    r.forwarder.stop(); // if a second interval leaked, stop would only clear one
    r.outbox.append([msg(1)]);
    t.mock.timers.tick(3 * INTERVAL_MS);

    assert.deepEqual(r.publisher.ids, []);
  });
});

describe("Forwarder: order", () => {
  test("batches are published oldest first", (t) => {
    const r = rig(t, { maxInFlight: 3 });
    r.outbox.append([1, 2, 3].map(msg));

    r.forwarder.start();

    assert.deepEqual(r.publisher.ids, ["batch-001", "batch-002", "batch-003"]);
    assert.deepEqual(
      r.publisher.attempts.map((a) => a.batch.messages[0]?.ts),
      [1, 2, 3],
    );
  });
});
