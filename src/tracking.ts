// Tracking: sample sources on a schedule, decide what to keep, hand kept
// samples to the outbox once per tick.
//
// Holds no state that matters across restarts. Everything here is memory.

import type { Message } from "./types.ts";
import type { Outbox } from "./outbox.ts";

/** A virtual data source: deterministic in time. */
export type Source = (unixTimeMs: number) => number;

export const sources: Record<string, Source> = {
  sin: (t) => Math.sin(t / 10000),
  cos: (t) => Math.cos(t / 10000),
};

/** One entry of config.json. */
export interface TrackerConfig {
  name: string;
  source: string;
  sampleMs: number;
  precision: number;
}

/** Two clocks: wall time for `ts`, monotonic time for "how long since". */
export interface Clock {
  now(): number;
  monotonic(): number;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  monotonic: () => performance.now(),
};

export class Tracker {
  constructor(_config: TrackerConfig, _source: Source, _heartbeatMs: number) {
    throw new Error("not implemented");
  }
  /** Sample if due. Returns a message when the sample is kept. */
  sample(_wallMs: number, _monoMs: number): Message | undefined {
    throw new Error("not implemented");
  }
}

export class Scheduler {
  constructor(_trackers: Tracker[], _outbox: Outbox, _clock: Clock, _tickMs: number) {
    throw new Error("not implemented");
  }
  start(): void {
    throw new Error("not implemented");
  }
  stop(): void {
    throw new Error("not implemented");
  }
}
