export class TimeoutError extends Error {
  constructor(ms: number) {
    super(`Timed out after ${ms}ms`);
    this.name = "TimeoutError";
  }
}
/**
 * Settle like `promise`, unless it is still pending after `ms`, in which
 * case reject with TimeoutError. The original promise is not cancelled
 * (nothing can cancel a promise); it keeps running in the background.
 */
export function withTimeOut<T>(ms: number, promise: Promise<T>): Promise<T> {
  // If the promise loses the race and rejects later, nobody is awaiting it
  // any more. Without a handler that is an unhandled rejection, which by
  // default crashes the process. This swallows it; the race already told
  // the caller everything it needs.
  promise.catch(() => {});

  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError(ms)), ms);
    // Don't keep the process alive just to time out an abandoned publish
    // after shutdown; anything still worth waiting for holds its own handle.
    timer.unref();
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
