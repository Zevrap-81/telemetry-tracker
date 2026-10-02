// Batch ids are UUIDv7 (RFC 9562): 48 bits of Unix milliseconds, then
// version and variant bits, then random. Because the timestamp comes first
// and is big-endian, sorting ids as strings sorts batches by creation time,
// so "oldest first" needs no extra column.
//
// Within one millisecond the timestamp alone can't order ids, so the 12 bits
// after the version nibble are a counter that increments while the clock
// stands still (the RFC's "method 1"). Ids from this process are therefore
// strictly increasing, even when several are made in the same millisecond.
//
// Layout, 16 bytes:
//   0..5   unix_ts_ms, big-endian
//   6..7   version nibble (0111) + 12-bit counter
//   8      variant bits (10) + 6 random bits
//   9..15  random

import { randomBytes, randomInt } from "node:crypto";

let lastMs = 0;
let counter = 0;

export function uuidv7(): string {
  let nowMs = Date.now();
  if (nowMs <= lastMs) {
    // Same millisecond, or the wall clock stepped backwards: stay monotonic.
    nowMs = lastMs;
    counter += 1;
    if (counter > 0x0fff) {
      // 4096 ids in one millisecond. Borrow the next millisecond.
      nowMs = lastMs + 1;
      counter = 0;
    }
  } else {
    // Start the counter at a random point so ids don't reveal batch counts.
    counter = randomInt(0, 0x0800);
  }
  lastMs = nowMs;

  const bytes = randomBytes(16);
  bytes.writeUIntBE(nowMs, 0, 6);
  bytes[6] = 0x70 | (counter >> 8);
  bytes[7] = counter & 0xff;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Read the millisecond timestamp back out of a UUIDv7. Used by tests. */
export function uuidv7TimeMs(id: string): number {
  return Number.parseInt(id.slice(0, 8) + id.slice(9, 13), 16);
}
