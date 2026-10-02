// Forwarding: claim unclaimed rows into batches, publish each batch to
// stdout, delete its rows only after publish resolved. Retry forever.

import type { Batch } from "./types.ts";
import type { Outbox } from "./outbox.ts";

export interface Publisher {
  /** Writes the batch to stdout as one JSON line. Fails at random. */
  publish(batch: Batch): Promise<void>;
}

export interface ForwarderConfig {
  batchSize: number;
  forwardIntervalMs: number; // also the linger
  maxInFlight: number;
  publishTimeoutMs: number;
}

export class Forwarder {
  constructor(_outbox: Outbox, _publisher: Publisher, _config: ForwarderConfig) {
    throw new Error("not implemented");
  }
  start(): void {
    throw new Error("not implemented");
  }
  stop(): void {
    throw new Error("not implemented");
  }
}
