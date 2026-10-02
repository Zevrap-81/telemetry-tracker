import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { TimeoutError, withTimeOut } from "../src/withTimeOut.ts";

/** A promise plus the handles to settle it from outside. */
function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (err: Error) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Let queued microtasks run. setImmediate is not faked by t.mock.timers. */
const drain = () => new Promise<void>((r) => setImmediate(r));

describe("withTimeOut", () => {
  test("resolves with the value when the promise settles first", async () => {
    const value = await withTimeOut(1_000, Promise.resolve(42));

    assert.equal(value, 42);
  });

  test("rejects with the promise's error when it rejects first", async () => {
    await assert.rejects(withTimeOut(1_000, Promise.reject(new Error("boom"))), /boom/);
  });

  test("rejects with TimeoutError when the promise is still pending after ms", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const never = new Promise<void>(() => {});

    const wrapped = withTimeOut(1_000, never);
    t.mock.timers.tick(999);
    // still pending: nothing to assert yet, the next tick decides
    t.mock.timers.tick(1);

    await assert.rejects(wrapped, TimeoutError);
  });

  test("clears the timer when the promise wins, so nothing fires later", async (t) => {
    const clear = t.mock.method(globalThis, "clearTimeout");

    await withTimeOut(60_000, Promise.resolve("fast"));

    assert.equal(clear.mock.callCount(), 1);
  });

  test("a promise that rejects after the timeout causes no unhandled rejection", async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const unhandled = t.mock.fn();
    process.on("unhandledRejection", unhandled);
    t.after(() => process.off("unhandledRejection", unhandled));
    const slow = deferred();

    const wrapped = withTimeOut(1_000, slow.promise);
    t.mock.timers.tick(1_000);
    await assert.rejects(wrapped, TimeoutError);

    slow.reject(new Error("too late")); // the loser of the race rejects now
    await drain();

    assert.equal(unhandled.mock.callCount(), 0);
  });
});
