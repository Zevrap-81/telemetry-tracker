import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  Scheduler,
  Tracker,
  type Clock,
  type TrackerConfig,
} from "../src/tracking.ts";
import { sources, type Source } from "../src/sources.ts";

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
