import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { uuidv7, uuidv7TimeMs } from "../src/batchId.ts";

const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("uuidv7", () => {
  test("has the RFC 9562 shape: version 7, variant 10", () => {
    assert.match(uuidv7(), UUID_V7);
  });

  test("embeds the current time in milliseconds", () => {
    const before = Date.now();
    const id = uuidv7();
    const after = Date.now();

    const embedded = uuidv7TimeMs(id);
    assert.ok(embedded >= before && embedded <= after + 1, `${embedded} not in [${before}, ${after}]`);
  });

  test("ids made in quick succession are strictly increasing as strings", () => {
    // Many of these land in the same millisecond, which is where the
    // counter matters. Without it this test fails about half the time.
    const ids = Array.from({ length: 1_000 }, () => uuidv7());

    for (let i = 1; i < ids.length; i++) {
      assert.ok(ids[i - 1]! < ids[i]!, `${ids[i - 1]} should sort before ${ids[i]}`);
    }
  });

  test("ids are unique", () => {
    const ids = new Set(Array.from({ length: 1_000 }, () => uuidv7()));
    assert.equal(ids.size, 1_000);
  });
});
