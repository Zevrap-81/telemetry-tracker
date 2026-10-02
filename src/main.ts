// Entry point. Wires config -> trackers -> outbox -> forwarder.
// stdout is reserved for batches; everything we say goes to stderr.

import { Tracker, Scheduler, systemClock } from "./tracking.ts";
import { SqliteOutbox } from "./outbox.ts";
import { Forwarder } from "./forwarding.ts";
import { StdoutPublisher } from "./publisher.ts";
import { loadSettings, loadTrackerConfigs, checkSettingsAgainstTrackers } from "./config.ts";

// 1. Input. Any problem throws a ConfigError listing everything wrong, which
//    ends the process with a non-zero exit before anything has started.
const settings = loadSettings();
const configs = loadTrackerConfigs(settings.configPath);
checkSettingsAgainstTrackers(settings, configs);

// 2. Build. Order matters only in that both stages share the outbox.
const outbox = new SqliteOutbox(settings.outboxPath);
console.error(
  `outbox: ${settings.outboxPath} journal_mode=${outbox.pragma("journal_mode")} synchronous=${outbox.pragma("synchronous")}`,
);

const trackers = configs.map((c) => new Tracker(c, settings.heartbeatMs));
const scheduler = new Scheduler(trackers, outbox, systemClock, settings.tickMs, settings.tickMs);

const publisher = new StdoutPublisher(settings.publishFailureRate);
const forwarder = new Forwarder(outbox, publisher, {
  batchSize: settings.batchSize,
  forwardIntervalMs: settings.forwardIntervalMs,
  maxInFlight: settings.maxInFlight,
  publishTimeoutMs: settings.publishTimeoutMs,
});

// 3. Run until told to stop.
scheduler.start();
forwarder.start();
console.error(
  `tracking ${trackers.length} trackers from ${settings.configPath}, tick ${settings.tickMs} ms; ` +
    `forwarding every ${settings.forwardIntervalMs} ms, batches of ${settings.batchSize}, ` +
    `max ${settings.maxInFlight} in flight, publish failure rate ${settings.publishFailureRate}`,
);

// 4. Shutdown. Stop the timers and close the database, then let Node exit on
//    its own once nothing is pending. No process.exit(): that could cut off a
//    stdout write still in the pipe. There is nothing to flush: every tick
//    that finished has already committed, and anything unacked is resent
//    after the next start. A hung publish holds its timeout timer, so exit
//    can take up to PUBLISH_TIMEOUT_MS.
function shutdown(signal: string): void {
  console.error(`${signal} received, stopping`);
  scheduler.stop();
  forwarder.stop();
  outbox.close();
}
process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));
