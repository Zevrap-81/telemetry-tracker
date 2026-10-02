import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  Scheduler,
  Tracker,
  type Clock,
  type TrackerConfig,
} from "../src/tracking.ts";
import { sources, type Source } from "../src/sources.ts";
import type { Message, Batch } from "../src/types.ts";
import type { Outbox } from "../src/outbox.ts";

// The identity source returns the wall time it was given, so the test
// controls the sampled value exactly by choosing wallMs. That makes
// precision boundaries testable, which sin and cos can't give us.
const identity: Source = (t) => t;

const HEARTBEAT_MS = 15_000;
const SAMPLE_MS = 1_000;
const PRECISION = 8;

function makeTracker(
  overrides: Partial<TrackerConfig> = {},
  source: Source = identity,
  heartbeatMs = HEARTBEAT_MS,
): Tracker {
  const config: TrackerConfig = {
    name: "t",
    source: "identity",
    sampleMs: SAMPLE_MS,
    precision: PRECISION,
    ...overrides,
  };
  return new Tracker(config, heartbeatMs, source);
}

describe("Tracker: first sample", () => {
  test("is always kept and carries wall time and the raw value", () => {
    const sin = sources["sin"]!;
    const tracker = makeTracker({ name: "one" }, sin);

    const msg = tracker.sample(1_000_000, 0);

    assert.deepEqual(msg, {
      name: "one",
      ts: 1_000_000,
      value: sin(1_000_000),
    });
  });
});

describe("Tracker: significant change", () => {
  test("a change below precision is dropped", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100 + PRECISION - 1, 1_000);

    assert.equal(msg, undefined);
  });

  test("a change of exactly precision is kept", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100 + PRECISION, 1_000);

    assert.deepEqual(msg, {
      name: "t",
      ts: 100 + PRECISION,
      value: 100 + PRECISION,
    });
  });

  test("a change in either direction counts", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100 - PRECISION, 1_000);

    assert.notEqual(msg, undefined);
  });

  test("the comparison is against the last kept value, not the last sampled one", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0); // kept
    tracker.sample(105, 1_000); // dropped, change 5
    tracker.sample(110, 2_000); // kept, change 10 from 100

    // Change from last kept (110) is 5, so drop.
    const msg = tracker.sample(115, 3_000);

    assert.equal(msg, undefined);
  });
});

describe("Tracker: heartbeat", () => {
  test("an unchanged value is kept once more than the heartbeat has passed", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100, HEARTBEAT_MS + 1);

    assert.deepEqual(msg, { name: "t", ts: 100, value: 100 });
  });

  test("an unchanged value is not kept at exactly the heartbeat", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100, HEARTBEAT_MS);

    assert.equal(msg, undefined);
  });

  test("the heartbeat is measured from the last kept sample, not the last sampled one", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0); // kept at 0
    tracker.sample(100, 10_000); // sampled, dropped: no change, 10 s < heartbeat

    // 16 s since last kept, 6 s since last sampled. Must be kept.
    const msg = tracker.sample(100, 16_000);

    assert.notEqual(msg, undefined);
  });
});

describe("Tracker: schedule", () => {
  test("is not due before its interval, even with a significant change", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100 + 10 * PRECISION, SAMPLE_MS - 1);

    assert.equal(msg, undefined);
  });

  test("is due at exactly its interval", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    const msg = tracker.sample(100 + PRECISION, SAMPLE_MS);

    assert.notEqual(msg, undefined);
  });

  test("missed intervals are skipped, not caught up", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    // The process stalled for 5 s. One sample now, then resume on the grid.
    assert.notEqual(tracker.sample(200, 5_000), undefined);
    assert.equal(tracker.sample(300, 5_100), undefined); // not due, despite big change
    assert.notEqual(tracker.sample(400, 6_000), undefined);
  });

  test("the schedule stays on its grid after a late tick", () => {
    const tracker = makeTracker();
    tracker.sample(100, 0);

    // Tick arrives 300 ms late. The next due time must be 2000, not 2300.
    tracker.sample(200, 1_300);
    assert.equal(tracker.sample(300, 1_999), undefined);
    assert.notEqual(tracker.sample(400, 2_000), undefined);
  });

  test("a tracker with a longer interval is not due on the base tick", () => {
    const tracker = makeTracker({ sampleMs: 2_000 });
    tracker.sample(100, 0);

    assert.equal(tracker.sample(200, 1_000), undefined);
    assert.notEqual(tracker.sample(200, 2_000), undefined);
  });
});

// ---------------------------------------------------------------------------
// Scheduler
//
// The scheduler is tested by calling tick() directly with a fake clock, so no
// test here waits on a real timer. Only the last test touches setInterval,
// and it does so through Node's built-in fake timers.
// ---------------------------------------------------------------------------

const TICK_MS = 100;

/** A clock the test moves by hand. Reset in each test; module state persists. */
const clock = { wall: 1_000_000, mono: 0 };
const fakeClock: Clock = {
  now: () => clock.wall,
  monotonic: () => clock.mono,
};

/** Records every append. The forwarding methods are never reached from here. */
class FakeOutbox implements Outbox {
  calls: Message[][] = [];
  failNextAppend = false;

  append(messages: Message[]): void {
    if (this.failNextAppend) {
      this.failNextAppend = false;
      throw new Error("disk on fire");
    }
    this.calls.push([...messages]);
  }
  claim(_batchSize: number): string | undefined {
    throw new Error("not used by the scheduler");
  }
  unpublishedBatches(_limit: number): Batch[] {
    throw new Error("not used by the scheduler");
  }
  ack(_batchId: string): void {
    throw new Error("not used by the scheduler");
  }
  close(): void {}
}

function makeScheduler(
  trackers: Tracker[],
  outbox: Outbox = new FakeOutbox(),
): Scheduler {
  return new Scheduler(trackers, outbox, fakeClock, TICK_MS, TICK_MS);
}

describe("Scheduler: one tick", () => {
  test("samples every tracker with the same wall time and appends them in one call", () => {
    clock.mono = 0;
    const outbox = new FakeOutbox();
    const scheduler = makeScheduler(
      [makeTracker({ name: "a" }), makeTracker({ name: "b" })],
      outbox,
    );

    scheduler.tick();

    assert.equal(outbox.calls.length, 1);
    assert.deepEqual(outbox.calls[0], [
      { name: "a", ts: clock.wall, value: clock.wall },
      { name: "b", ts: clock.wall, value: clock.wall },
    ]);
  });

  test("does not call append when no tracker kept anything", () => {
    clock.mono = 0;
    const outbox = new FakeOutbox();
    const scheduler = makeScheduler([makeTracker()], outbox);
    scheduler.tick();

    clock.mono = SAMPLE_MS / 2; // nothing is due yet
    scheduler.tick();

    assert.equal(outbox.calls.length, 1);
  });

  test("appends only this tick's messages, never earlier ones", () => {
    clock.mono = 0;
    const outbox = new FakeOutbox();
    const scheduler = makeScheduler([makeTracker()], outbox);
    scheduler.tick();

    clock.mono = SAMPLE_MS;
    clock.wall += 10 * PRECISION; // a significant change, so it is kept
    scheduler.tick();

    assert.equal(outbox.calls.length, 2);
    assert.equal(outbox.calls[1]!.length, 1);
  });

  test("samples each tracker only when its own interval is due", () => {
    clock.mono = 0;
    const outbox = new FakeOutbox();
    const scheduler = makeScheduler(
      [
        makeTracker({ name: "fast", sampleMs: 1_000 }),
        makeTracker({ name: "slow", sampleMs: 2_000 }),
      ],
      outbox,
    );
    const kept = () => outbox.calls.at(-1)!.map((m) => m.name);

    scheduler.tick();
    assert.deepEqual(kept(), ["fast", "slow"]);

    clock.mono = 1_000;
    clock.wall += 10 * PRECISION;
    scheduler.tick();
    assert.deepEqual(kept(), ["fast"]);

    clock.mono = 2_000;
    clock.wall += 10 * PRECISION;
    scheduler.tick();
    assert.deepEqual(kept(), ["fast", "slow"]);
  });

  test("an append that throws is logged and the next tick still works", () => {
    clock.mono = 0;
    const outbox = new FakeOutbox();
    const scheduler = makeScheduler([makeTracker()], outbox);

    outbox.failNextAppend = true;
    scheduler.tick(); // must not throw
    assert.equal(outbox.calls.length, 0);

    clock.mono = SAMPLE_MS;
    clock.wall += 10 * PRECISION;
    scheduler.tick();
    assert.equal(outbox.calls.length, 1);
  });
});

describe("Scheduler: start and stop", () => {
  test("start drives tick through setInterval and stop ends it", (t) => {
    t.mock.timers.enable({ apis: ["setInterval"] });
    clock.mono = 0;
    const outbox = new FakeOutbox();
    // A tracker with sampleMs equal to the tick, so every tick keeps one message.
    const scheduler = makeScheduler(
      [makeTracker({ sampleMs: TICK_MS, precision: 0 })],
      outbox,
    );

    scheduler.start();
    for (let i = 1; i <= 3; i++) {
      clock.mono = i * TICK_MS;
      t.mock.timers.tick(TICK_MS);
    }
    assert.equal(outbox.calls.length, 3);

    scheduler.stop();
    clock.mono = 10 * TICK_MS;
    t.mock.timers.tick(5 * TICK_MS);
    assert.equal(outbox.calls.length, 3);
  });
});
