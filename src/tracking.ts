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
      throw new Error(`unknown source ${config.source}`);
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
  private readonly trackers: Tracker[];
  private readonly outbox: Outbox;
  private readonly clock: Clock;
  private readonly tickMs: number;
  private readonly thresholdMs: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private expectedMono: number;
  constructor(
    trackers: Tracker[],
    outbox: Outbox,
    clock: Clock,
    tickMs: number,
    thresholdMs: number,
  ) {
    // throw new Error("not implemented");
    this.trackers = trackers;
    this.outbox = outbox;
    this.clock = clock;
    this.tickMs = tickMs;
    this.thresholdMs = thresholdMs;
    this.expectedMono = this.clock.monotonic();
  }
  start(): void {
    // throw new Error("not implemented");
    this.expectedMono += this.tickMs;
    this.timer = setInterval(() => this.tick(), this.tickMs);
  }
  stop(): void {
    // throw new Error("not implemented");
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
  tick(): void {
    const clockNow = this.clock.now();
    const clockMono = this.clock.monotonic();

    const delay = clockMono - this.expectedMono;
    if (delay > this.thresholdMs) {
      console.error(`The tick was late by ${delay}ms`);
    }

    const pending: Message[] = [];
    for (const tracker of this.trackers) {
      const sample_message = tracker.sample(clockNow, clockMono);
      if (sample_message !== undefined) pending.push(sample_message);
    }

    if (pending.length > 0) {
      try {
        this.outbox.append(pending);
      } catch (err) {
        console.error("outbox append failed, dropping tick", err);
      }
    }

    this.expectedMono = clockMono + this.tickMs;
  }
}
