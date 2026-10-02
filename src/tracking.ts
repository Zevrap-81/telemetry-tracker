// Tracking: sample sources on a schedule, decide what to keep, hand kept
// samples to the outbox once per tick.
//
// Holds no state that matters across restarts. Everything here is memory.

import type { Message } from "./types.ts";
import type { Outbox } from "./outbox.ts";
import { sources, type Source } from "./sources.ts";

/** One entry of config.json. */
export interface TrackerConfig {
  name: string;
  source: string;
  sampleMs: number;
  precision: number;
}

/** Two clocks: wall time for `ts`, monotonic time for "how long since". */
export interface Clock {
  now: () => number;
  monotonic: () => number;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  monotonic: () => performance.now(),
};

export class Tracker {
  private readonly name: string;
  private readonly source: Source;
  private readonly sampleMs: number;
  private readonly precision: number;
  private readonly heartbeatMs: number;
  private lastKept: { value: number; atMono: number } | null = null;
  private nextDueMono: number = 0;

  constructor(
    config: TrackerConfig,
    heartbeatMs: number,
    source: Source | null = null,
  ) {
    // throw new Error("not implemented");
    this.name = config.name;
    this.sampleMs = config.sampleMs;
    this.precision = config.precision;
    this.heartbeatMs = heartbeatMs;
    if (sources[config.source] != null) {
      this.source = sources[config.source]!;
    } else if (source != null) {
      this.source = source;
    } else {
      throw `unknown source ${config.source}`;
    }
  }
  /** Sample if due. Returns a message when the sample is kept. */
  sample(wallMs: number, monoMs: number): Message | undefined {
    // throw new Error("not implemented");
    let kept: Message | undefined = undefined;
    const newSample = this.source(wallMs);

    if (!this.isDue(monoMs)) return undefined;

    if (
      this.lastKept === null ||
      this.lastKept.atMono + this.heartbeatMs < monoMs ||
      Math.abs(this.lastKept.value - newSample) >= this.precision
    ) {
      kept = { name: this.name, ts: wallMs, value: newSample };
      this.lastKept = { value: newSample, atMono: monoMs };
    }

    // update nextDueMono incase of late sample
    while (this.isDue(monoMs)) {
      this.nextDueMono += this.sampleMs;
    }
    return kept;
  }

  private isDue(monoMs: number): boolean {
    return this.nextDueMono <= monoMs;
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
