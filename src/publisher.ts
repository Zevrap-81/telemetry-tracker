// Publisher: the unreliable channel to "the cloud", which is stdout.
//
// One batch is one JSON line, written with a single process.stdout.write
// call so concurrent publishes cannot interleave. Failures are injected at
// random, as the brief asks: with probability `failureRate` the publish
// rejects without writing, never settles, or writes and then rejects.

import type { Batch } from "./types.ts";
import type { Publisher } from "./forwarding.ts";

export type FailureMode = "reject" | "hang" | "write-then-reject";
const FAILURE_MODES: readonly FailureMode[] = ["reject", "hang", "write-then-reject"];

/** Where lines go. Resolves once the line has been handed to the OS. */
export type Sink = (line: string) => Promise<void>;

export class StdoutPublisher implements Publisher {
  private readonly failureRate: number;
  private readonly random: () => number;
  private readonly sink: Sink;

  /**
   * @param failureRate probability in [0, 1] that a publish fails
   * @param random source of numbers in [0, 1); injectable so tests can force a mode
   * @param sink where lines are written; injectable so tests never touch process.stdout
   */
  constructor(failureRate: number, random: () => number = Math.random, sink: Sink = stdoutSink) {
    this.failureRate = failureRate;
    this.random = random;
    this.sink = sink;
  }

  publish(batch: Batch): Promise<void> {
    const line = JSON.stringify(batch) + "\n";

    if (this.random() < this.failureRate) {
      const mode = FAILURE_MODES[Math.floor(this.random() * FAILURE_MODES.length)]!;
      switch (mode) {
        case "reject":
          return Promise.reject(new Error("publish failed: connection refused"));
        case "hang":
          return new Promise<void>(() => {});
        case "write-then-reject":
          // The receiver got it; the acknowledgement was lost. The forwarder
          // will resend under the same batch_id and the receiver dedupes.
          return this.sink(line).then(() => {
            throw new Error("publish failed: ack lost");
          });
      }
    }
    return this.sink(line);
  }
}

/** The real sink: one write call per line, resolved from the write callback. */
export function stdoutSink(line: string): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    // The boolean return value is backpressure advice (buffer above the
    // high-water mark), not a failure, so it is ignored here. The callback
    // is the signal that the bytes left the process.
    process.stdout.write(line, (err) => (err ? reject(err) : resolve()));
  });
}
