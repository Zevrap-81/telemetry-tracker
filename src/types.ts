// Shared data shapes. Nothing here has behaviour.

/** One kept sample. Identified by `name` + `ts`. */
export interface Message {
  name: string;
  ts: number; // unix time in milliseconds when the sample was taken
  value: number;
}

/** What `publish` sends: one JSON line on stdout. */
export interface Batch {
  batch_id: string;
  messages: Message[];
}
