import { describe, test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ConfigError,
  DEFAULT_SETTINGS,
  checkSettingsAgainstTrackers,
  loadSettings,
  loadTrackerConfigs,
} from "../src/config.ts";

function writeTempConfig(t: TestContext, content: unknown): string {
  const dir = mkdtempSync(join(tmpdir(), "config-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "config.json");
  writeFileSync(path, JSON.stringify(content));
  return path;
}

const valid = [
  { name: "one", source: "sin", sampleMs: 1000, precision: 0.08 },
  { name: "two", source: "cos", sampleMs: 2000, precision: 0.15 },
];

describe("loadTrackerConfigs", () => {
  test("loads a valid config unchanged", (t) => {
    assert.deepEqual(loadTrackerConfigs(writeTempConfig(t, valid)), valid);
  });

  test("reports every problem at once, not just the first", (t) => {
    const path = writeTempConfig(t, [
      { name: "one", source: "tan", sampleMs: 500, precision: -1 },
      { name: "one", source: "sin", sampleMs: 1000, precision: 1 },
    ]);

    assert.throws(
      () => loadTrackerConfigs(path),
      (err: unknown) => {
        assert.ok(err instanceof ConfigError);
        assert.match(err.message, /unknown source "tan"/);
        assert.match(err.message, /sampleMs.*500/);
        assert.match(err.message, /precision.*-1/);
        assert.match(err.message, /duplicate name "one"/);
        return true;
      },
    );
  });

  test("rejects sampleMs below 1000 and accepts exactly 1000", (t) => {
    assert.throws(() => loadTrackerConfigs(writeTempConfig(t, [{ ...valid[0], sampleMs: 999 }])), ConfigError);
    assert.doesNotThrow(() => loadTrackerConfigs(writeTempConfig(t, [{ ...valid[0], sampleMs: 1000 }])));
  });

  test("inherited object keys are not sources", (t) => {
    // `in` and plain indexing see Object.prototype; "toString" would pass
    // and install a prototype method as the sampling function.
    for (const bad of ["toString", "__proto__", "constructor", "hasOwnProperty"]) {
      const path = writeTempConfig(t, [{ ...valid[0], source: bad }]);
      assert.throws(() => loadTrackerConfigs(path), /unknown source/, bad);
    }
  });

  test("rejects a file that is not an array", (t) => {
    assert.throws(() => loadTrackerConfigs(writeTempConfig(t, { name: "one" })), /expected a JSON array/);
  });

  test("accepts an empty list", (t) => {
    assert.deepEqual(loadTrackerConfigs(writeTempConfig(t, [])), []);
  });
});

describe("loadSettings", () => {
  test("uses the defaults when the environment is empty", () => {
    assert.deepEqual(loadSettings({}), DEFAULT_SETTINGS);
  });

  test("reads numbers and paths from the environment", () => {
    const s = loadSettings({ TICK_MS: "250", MAX_IN_FLIGHT: "3", PUBLISH_FAILURE_RATE: "0", OUTBOX_PATH: "/tmp/x.sqlite" });

    assert.equal(s.tickMs, 250);
    assert.equal(s.maxInFlight, 3);
    assert.equal(s.publishFailureRate, 0);
    assert.equal(s.outboxPath, "/tmp/x.sqlite");
  });

  test("rejects values that are not numbers, not positive, or out of range, all at once", () => {
    assert.throws(
      () => loadSettings({ TICK_MS: "fast", BATCH_SIZE: "0", PUBLISH_FAILURE_RATE: "1.5" }),
      (err: unknown) => {
        assert.ok(err instanceof ConfigError);
        assert.match(err.message, /TICK_MS/);
        assert.match(err.message, /BATCH_SIZE/);
        assert.match(err.message, /PUBLISH_FAILURE_RATE/);
        return true;
      },
    );
  });

  test("an empty string means unset", () => {
    assert.equal(loadSettings({ TICK_MS: "" }).tickMs, DEFAULT_SETTINGS.tickMs);
  });
});

describe("checkSettingsAgainstTrackers", () => {
  test("the tick must be below the smallest sampleMs", () => {
    const settings = { ...DEFAULT_SETTINGS, tickMs: 1000 };

    assert.throws(() => checkSettingsAgainstTrackers(settings, valid), /TICK_MS=1000 must be below/);
    assert.doesNotThrow(() => checkSettingsAgainstTrackers({ ...settings, tickMs: 999 }, valid));
  });

  test("an empty tracker list passes", () => {
    assert.doesNotThrow(() => checkSettingsAgainstTrackers(DEFAULT_SETTINGS, []));
  });
});
