// Outbox: the durable store between tracking and forwarding.
//
// Backed by SQLite (node:sqlite, built into Node 24). Every method is
// synchronous and returns only after its transaction is committed and
// fsynced, so "the call returned" means "it is on disk".

import type { Message, Batch } from "./types.ts";

export interface Outbox {
  /** Store kept samples. One transaction, one fsync. */
  append(messages: Message[]): void;

  /**
   * Stamp up to `batchSize` oldest unclaimed rows with a fresh batch id.
   * Returns the id, or undefined if there was nothing to claim.
   * A row gets a batch id exactly once; it is never reassigned.
   */
  claim(batchSize: number): string | undefined;

  /** Claimed batches that have not been acked yet, oldest first. */
  unpublishedBatches(limit: number): Batch[];

  /** Delete a batch's rows. Only called after publish resolved. */
  ack(batchId: string): void;

  close(): void;
}

export class SqliteOutbox implements Outbox {
  constructor(_path: string) {
    throw new Error("not implemented");
  }
  append(_messages: Message[]): void {
    throw new Error("not implemented");
  }
  claim(_batchSize: number): string | undefined {
    throw new Error("not implemented");
  }
  unpublishedBatches(_limit: number): Batch[] {
    throw new Error("not implemented");
  }
  ack(_batchId: string): void {
    throw new Error("not implemented");
  }
  close(): void {
    throw new Error("not implemented");
  }
}
