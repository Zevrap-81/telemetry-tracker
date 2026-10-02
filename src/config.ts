// Config: turn the outside world (config.json, environment) into validated,
// typed values. Everything that can be wrong with the input is reported here,
// all at once, before anything else starts.

import { readFileSync } from "node:fs";
import type { TrackerConfig } from "./tracking.ts";
import { sources, lookupSource } from "./sources.ts";

/** Application settings. config.json stays in the brief's format; these come from the environment. */
export interface Settings {
  configPath: string;
  outboxPath: string;
  tickMs: number;
  heartbeatMs: number;
  batchSize: number;
  forwardIntervalMs: number;
  maxInFlight: number;
  publishTimeoutMs: number;
  publishFailureRate: number;
}

export const DEFAULT_SETTINGS: Settings = {
  configPath: "./config.json",
  outboxPath: "./outbox.sqlite",
  tickMs: 100,
  heartbeatMs: 15_000,
  batchSize: 5_000,
  forwardIntervalMs: 10_000,
  maxInFlight: 1,
  publishTimeoutMs: 5_000,
  publishFailureRate: 0.3,
};

/** Thrown for any invalid input. `message` lists every problem found, one per line. */
export class ConfigError extends Error {
  constructor(problems: string[]) {
    super(problems.join("\n"));
    this.name = "ConfigError";
  }
}

/** Read and validate the tracker list. Throws ConfigError listing every problem. */
export function loadTrackerConfigs(path: string): TrackerConfig[] {
  const raw: unknown = JSON.parse(readFileSync(path, "utf8"));
  if (!Array.isArray(raw)) {
    throw new ConfigError([`${path}: expected a JSON array of trackers`]);
  }
  const problems: string[] = [];
  const seen = new Set<string>();
  const configs: TrackerConfig[] = [];

  raw.forEach((entry: unknown, i: number) => {
    const where = `${path}[${i}]`;
    if (typeof entry !== "object" || entry === null) {
      problems.push(`${where}: expected an object`);
      return;
    }
    const e = entry as Record<string, unknown>;
    const { name, source, sampleMs, precision } = e;

    if (typeof name !== "string" || name === "") {
      problems.push(`${where}: "name" must be a non-empty string`);
    } else if (seen.has(name)) {
      problems.push(`${where}: duplicate name "${name}"`);
    } else {
      seen.add(name);
    }
    if (typeof source !== "string" || lookupSource(source) === undefined) {
      problems.push(`${where}: unknown source ${JSON.stringify(source)}, expected one of ${Object.keys(sources).join(", ")}`);
    }
    if (typeof sampleMs !== "number" || !Number.isFinite(sampleMs) || sampleMs < 1000) {
      problems.push(`${where}: "sampleMs" must be a number >= 1000, got ${JSON.stringify(sampleMs)}`);
    }
    if (typeof precision !== "number" || !Number.isFinite(precision) || precision < 0) {
      problems.push(`${where}: "precision" must be a number >= 0, got ${JSON.stringify(precision)}`);
    }
    configs.push(e as unknown as TrackerConfig);
  });

  if (problems.length > 0) throw new ConfigError(problems);
  return configs;
}

/** Read settings from the environment, falling back to DEFAULT_SETTINGS. */
export function loadSettings(env: NodeJS.ProcessEnv = process.env): Settings {
  const problems: string[] = [];

  const str = (name: string, fallback: string): string => env[name] ?? fallback;
  const num = (name: string, fallback: number, check: (n: number) => boolean, rule: string): number => {
    const raw = env[name];
    if (raw === undefined || raw === "") return fallback;
    const n = Number(raw);
    if (!Number.isFinite(n) || !check(n)) {
      problems.push(`${name}=${JSON.stringify(raw)}: ${rule}`);
      return fallback;
    }
    return n;
  };
  const positiveInt = (n: number) => Number.isInteger(n) && n > 0;
  const d = DEFAULT_SETTINGS;

  const settings: Settings = {
    configPath: str("CONFIG_PATH", d.configPath),
    outboxPath: str("OUTBOX_PATH", d.outboxPath),
    tickMs: num("TICK_MS", d.tickMs, positiveInt, "must be a positive integer"),
    heartbeatMs: num("HEARTBEAT_MS", d.heartbeatMs, positiveInt, "must be a positive integer"),
    batchSize: num("BATCH_SIZE", d.batchSize, positiveInt, "must be a positive integer"),
    forwardIntervalMs: num("FORWARD_INTERVAL_MS", d.forwardIntervalMs, positiveInt, "must be a positive integer"),
    maxInFlight: num("MAX_IN_FLIGHT", d.maxInFlight, positiveInt, "must be a positive integer"),
    publishTimeoutMs: num("PUBLISH_TIMEOUT_MS", d.publishTimeoutMs, positiveInt, "must be a positive integer"),
    publishFailureRate: num("PUBLISH_FAILURE_RATE", d.publishFailureRate, (n) => n >= 0 && n <= 1, "must be between 0 and 1"),
  };

  if (problems.length > 0) throw new ConfigError(problems);
  return settings;
}

/**
 * Checks that span both inputs. The tick must be finer than every sampling
 * interval, otherwise a tracker is sampled late and its interval aliases.
 */
export function checkSettingsAgainstTrackers(settings: Settings, configs: TrackerConfig[]): void {
  const smallest = Math.min(...configs.map((c) => c.sampleMs));
  if (configs.length > 0 && settings.tickMs >= smallest) {
    throw new ConfigError([
      `TICK_MS=${settings.tickMs} must be below the smallest sampleMs (${smallest})`,
    ]);
  }
}
