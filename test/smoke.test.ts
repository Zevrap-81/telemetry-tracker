// Proves the toolchain: TypeScript runs directly, node:test works,
// node:sqlite is available.

import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { sources } from "../src/sources.ts";

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
