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

// stdout is the network. A consumer that closes the pipe (EPIPE) surfaces
// both through the write callback, which rejects the publish so it is
// retried, and as an 'error' event on the stream. Without a listener that
// event is fatal, which would turn "channel down" into "process down".
process.stdout.on("error", (err: Error) => {
  console.error("stdout error, publishes will be retried:", err.message);
});

// 3. Run until told to stop.
scheduler.start();
forwarder.start();
console.error(
  `tracking ${trackers.length} trackers from ${settings.configPath}, tick ${settings.tickMs} ms; ` +
    `forwarding every ${settings.forwardIntervalMs} ms, batches of ${settings.batchSize}, ` +
    `max ${settings.maxInFlight} in flight, publish failure rate ${settings.publishFailureRate}`,
);

// 4. Shutdown. Stop the ticks, wait for publishes already in flight to
//    settle or time out, then close the database and let Node exit on its
//    own. No process.exit(): that could cut off a stdout write still in the
//    pipe. Nothing needs flushing: every tick that finished has already
//    committed, and anything unacked is resent after the next start.
//    Idempotent, since a supervisor may send SIGTERM after an operator's
//    SIGINT.
let stopping = false;
async function shutdown(signal: string): Promise<void> {
  if (stopping) return;
  stopping = true;
  console.error(`${signal} received, stopping`);
  scheduler.stop();
  await forwarder.stop();
  outbox.close();
}
process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
