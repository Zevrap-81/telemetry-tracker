import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { StdoutPublisher, type Sink } from "../src/publisher.ts";
import type { Batch } from "../src/types.ts";

const batch: Batch = { batch_id: "b-1", messages: [{ name: "one", ts: 1, value: 0.5 }] };

/** A sink that records lines instead of writing to stdout. */
function recordingSink(): { written: string[]; sink: Sink } {
  const written: string[] = [];
  return { written, sink: (line) => (written.push(line), Promise.resolve()) };
}

/** A random source that returns the given values in order. */
function scripted(...values: number[]): () => number {
  let i = 0;
  return () => values[i++] ?? 0;
}

describe("StdoutPublisher", () => {
  test("writes the batch as one JSON line and resolves", async () => {
    const { written, sink } = recordingSink();
    const publisher = new StdoutPublisher(0, Math.random, sink);

    await publisher.publish(batch);

    assert.deepEqual(written, [JSON.stringify(batch) + "\n"]);
    assert.deepEqual(JSON.parse(written[0]!), batch);
  });

  test("never fails with failureRate 0, always fails with failureRate 1", async () => {
    const { sink } = recordingSink();
    await assert.doesNotReject(new StdoutPublisher(0, () => 0, sink).publish(batch));
    await assert.rejects(new StdoutPublisher(1, scripted(0.5, 0), sink).publish(batch));
  });

  test("mode reject: rejects and writes nothing", async () => {
    const { written, sink } = recordingSink();
    // first draw decides failure (0 < rate), second picks the mode: 0 -> "reject"
    const publisher = new StdoutPublisher(0.5, scripted(0, 0), sink);

    await assert.rejects(publisher.publish(batch), /connection refused/);
    assert.deepEqual(written, []);
  });

  test("mode hang: never settles and writes nothing", async () => {
    const { written, sink } = recordingSink();
    const publisher = new StdoutPublisher(0.5, scripted(0, 0.4), sink); // 0.4 * 3 -> index 1 -> "hang"

    const settled = await Promise.race([
      publisher.publish(batch).then(() => "settled", () => "settled"),
      new Promise<string>((r) => setTimeout(() => r("pending"), 20)),
    ]);

    assert.equal(settled, "pending");
    assert.deepEqual(written, []);
  });

  test("mode write-then-reject: the line is on stdout and the promise rejects", async () => {
    const { written, sink } = recordingSink();
    const publisher = new StdoutPublisher(0.5, scripted(0, 0.9), sink); // 0.9 * 3 -> index 2

    await assert.rejects(publisher.publish(batch), /ack lost/);
    assert.deepEqual(written, [JSON.stringify(batch) + "\n"]);
  });

  test("a failed write rejects the publish", async () => {
    const broken: Sink = () => Promise.reject(new Error("EPIPE"));

    await assert.rejects(new StdoutPublisher(0, Math.random, broken).publish(batch), /EPIPE/);
  });
});
