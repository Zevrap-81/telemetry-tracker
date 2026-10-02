// Outbox: the durable store between tracking and forwarding.
//
// Backed by SQLite (node:sqlite, built into Node 24). Every method is
// synchronous and returns only after its transaction is committed and
// fsynced, so "the call returned" means "it is on disk".

import { DatabaseSync, StatementSync } from "node:sqlite";
import type { Message, Batch } from "./types.ts";
import { uuidv7 } from "./batchId.ts";

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

/** One row of the outbox table, as SQLite hands it back. */
interface Row {
  name: string;
  ts: number;
  value: number;
}

export class SqliteOutbox implements Outbox {
  private readonly db: DatabaseSync;
  private readonly insertStatement: StatementSync;
  private readonly claimStatement: StatementSync;
  private readonly batchIdsStatement: StatementSync;
  private readonly batchRowsStatement: StatementSync;
  private readonly ackStatement: StatementSync;
  private readonly batchIdGenerator: () => string;

  constructor(path: string, batchIdGenerator: () => string = uuidv7) {
    this.db = new DatabaseSync(path);
    // WAL: writers append to a log instead of rewriting pages in place.
    // synchronous=FULL: every COMMIT fsyncs that log before returning.
    // Together they are the durability guarantee. journal_mode persists in
    // the file; synchronous is per connection and must be set on every open.
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = FULL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS outbox (
        seq     INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT    NOT NULL,
        ts      INTEGER NOT NULL,
        value   REAL    NOT NULL,
        batchId TEXT,
        UNIQUE (ts, name)
      );
      CREATE INDEX IF NOT EXISTS outbox_unclaimed ON outbox (seq)     WHERE batchId IS NULL;
      CREATE INDEX IF NOT EXISTS outbox_claimed   ON outbox (batchId) WHERE batchId IS NOT NULL;
    `);
    // seq gives insertion order; UNIQUE(ts, name) is the spec's message
    // identity, and INSERT OR IGNORE makes a repeat of it a no-op.
    this.insertStatement = this.db.prepare(
      "INSERT OR IGNORE INTO outbox (name, ts, value) VALUES (?, ?, ?)",
    );
    this.claimStatement = this.db.prepare(`
      UPDATE outbox SET batchId = ?
      WHERE seq IN (SELECT seq FROM outbox WHERE batchId IS NULL ORDER BY seq LIMIT ?)
    `);
    // UUIDv7 sorts by creation time, so ORDER BY batchId is oldest first.
    this.batchIdsStatement = this.db.prepare(`
      SELECT DISTINCT batchId FROM outbox WHERE batchId IS NOT NULL ORDER BY batchId LIMIT ?
    `);
    this.batchRowsStatement = this.db.prepare(
      "SELECT name, ts, value FROM outbox WHERE batchId = ? ORDER BY seq",
    );
    this.ackStatement = this.db.prepare("DELETE FROM outbox WHERE batchId = ?");
    this.batchIdGenerator = batchIdGenerator;
  }

  /** Read a pragma value, for tests and the startup log line. */
  pragma(name: string): unknown {
    const row = this.db.prepare(`PRAGMA ${name}`).get() as Record<string, unknown>;
    return row[name];
  }

  append(messages: Message[]): void {
    // One transaction for the whole tick: one fsync, and all-or-nothing.
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const message of messages) {
        this.insertStatement.run(message.name, message.ts, message.value);
      }
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
  }

  claim(batchSize: number): string | undefined {
    // A single UPDATE is its own transaction. The id is generated first so
    // the rows and the id become durable together or not at all.
    const batchId = this.batchIdGenerator();
    const { changes } = this.claimStatement.run(batchId, batchSize);
    return changes > 0 ? batchId : undefined;
  }

  unpublishedBatches(limit: number): Batch[] {
    const ids = this.batchIdsStatement.all(limit) as { batchId: string }[];
    return ids.map(({ batchId }) => ({
      batch_id: batchId,
      // Copy into plain objects: building a message explicitly from the SQLite row
      // makes the test with deepEqual work.
      messages: (this.batchRowsStatement.all(batchId) as unknown as Row[]).map(
        ({ name, ts, value }): Message => ({ name, ts, value }),
      ),
    }));
  }

  ack(batchId: string): void {
    // Deleting zero rows is fine: a late settle of an already acked batch.
    this.ackStatement.run(batchId);
  }

  close(): void {
    // Idempotent: shutdown may run more than once (SIGINT, then SIGTERM).
    if (this.db.isOpen) this.db.close();
  }
}
