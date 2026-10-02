// Forwarding: claim unclaimed rows into batches, publish each batch to
// stdout, delete its rows only after publish resolved. Retry forever.

import type { Batch } from "./types.ts";
import type { Outbox } from "./outbox.ts";
import { TimeoutError, withTimeOut } from "./withTimeOut.ts";

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
  private readonly config: ForwarderConfig;
  private readonly outbox: Outbox;
  private readonly publisher: Publisher;
  /** Batch ids with a publish started and not yet settled. Written only by dispatch(). */
  private readonly inFlight: Set<string> = new Set();
  /** The dispatch promises behind inFlight, so stop() can wait for them. */
  private readonly pending: Set<Promise<void>> = new Set();
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(outbox: Outbox, publisher: Publisher, config: ForwarderConfig) {
    this.config = config;
    this.outbox = outbox;
    this.publisher = publisher;
  }

  start(): void {
    if (this.timer !== null) return;
    // Forward once right away: after a restart the outbox may hold a backlog,
    // and there is no reason to wait a full interval before draining it.
    this.tick();
    this.timer = setInterval(() => this.tick(), this.config.forwardIntervalMs);
  }

  /**
   * Stops the ticks at once. The returned promise settles when every publish
   * that was in flight has settled or timed out, so the caller can close the
   * outbox afterwards without an ack landing on a closed database. Bounded
   * by publishTimeoutMs; never rejects.
   */
  stop(): Promise<void> {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    return Promise.allSettled(this.pending).then(() => undefined);
  }

  /** One tick: claim everything unclaimed, then fill the free publish slots. */
  private tick(): void {
    // 1. Claim. Every unclaimed row gets a batch id now, so the forward
    //    interval doubles as the linger. Claims are independent of
    //    publishing, so a failure here must not stop the publish half.
    try {
      while (this.outbox.claim(this.config.batchSize) !== undefined) {
        // keep claiming until nothing is left
      }
    } catch (err) {
      console.error("forwarding: claim failed", err);
    }

    // 2. Fill free slots, oldest batch first, skipping batches already in
    //    flight. The in-flight batches are the oldest ones (they were claimed
    //    earlier), so fetch free + inFlight.size to be sure that after
    //    skipping them there are still `free` candidates left. Fetching only
    //    `free` would return exactly the in-flight ones and start nothing.
    let free = this.config.maxInFlight - this.inFlight.size;
    if (free <= 0) return;
    try {
      const candidates = this.outbox.unpublishedBatches(free + this.inFlight.size);
      for (const batch of candidates) {
        if (free === 0) break;
        if (this.inFlight.has(batch.batch_id)) continue;
        const run = this.dispatch(batch);
        this.pending.add(run);
        void run.finally(() => this.pending.delete(run));
        free -= 1;
      }
    } catch (err) {
      console.error("forwarding: unpublished batches failed", err);
    }
  }

  /**
   * The whole life of one publish attempt. Resolved: ack, so the rows are
   * gone. Rejected or timed out: leave the rows, the next tick retries under
   * the same batch id. Either way the slot is freed. Never rejects.
   */
  private async dispatch(batch: Batch): Promise<void> {
    const id = batch.batch_id;
    this.inFlight.add(id);
    try {
      await withTimeOut(this.config.publishTimeoutMs, this.publisher.publish(batch));
      this.outbox.ack(id);
    } catch (err) {
      const why = err instanceof TimeoutError ? "timed out" : "rejected";
      console.error(`forwarding: publish of ${id} ${why}, will retry`, err);
    } finally {
      this.inFlight.delete(id);
    }
  }
}
