// Entry point. Wires config -> trackers -> outbox -> forwarder.
// stdout is reserved for batches; everything we say goes to stderr.

import { readFileSync } from "node:fs";
import type { TrackerConfig } from "./tracking.ts";

const configPath = process.env["CONFIG_PATH"] ?? "./config.json";
const config = JSON.parse(readFileSync(configPath, "utf8")) as TrackerConfig[];

console.error(`loaded ${config.length} trackers from ${configPath}`);
console.error("nothing else is implemented yet");
