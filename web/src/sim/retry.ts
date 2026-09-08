// Port of conduit/core/retry.py: classification and exponential backoff with full jitter.

import type { RetryPolicy } from "./specs";
import type { Rng } from "./prng";

export class DeliveryError extends Error {
  readonly retryable: boolean = false;
  constructor(message: string, readonly status: number | null = null) {
    super(message);
    this.name = "DeliveryError";
  }
}

/** 429, 5xx, timeouts, connection resets: retry with backoff. */
export class TransientError extends DeliveryError {
  override readonly retryable = true;
  constructor(message: string, status: number | null = null, readonly retryAfter: number | null = null) {
    super(message, status);
    this.name = "TransientError";
  }
}

/** Hard 4xx (bad payload, auth, not found): do not retry, let SQS redrive. */
export class PermanentError extends DeliveryError {
  override readonly retryable = false;
  constructor(message: string, status: number | null = null) {
    super(message, status);
    this.name = "PermanentError";
  }
}

export interface FakeResponse {
  status: number;
  headers: Record<string, string>;
  body: Record<string, unknown>;
}

export function classifyResponse(response: FakeResponse, policy: RetryPolicy): DeliveryError | null {
  const status = response.status;
  if (status >= 200 && status < 300) return null;
  const body = JSON.stringify(response.body).slice(0, 200);
  if (policy.retryOnStatus.includes(status) || status >= 500) {
    return new TransientError(`HTTP ${status}: ${body}`, status, parseRetryAfter(response.headers["Retry-After"]));
  }
  return new PermanentError(`HTTP ${status}: ${body}`, status);
}

export function classifyException(exc: unknown): DeliveryError {
  if (exc instanceof DeliveryError) return exc;
  const name = exc instanceof Error ? exc.name : "Error";
  const message = exc instanceof Error ? exc.message : String(exc);
  if (/Timeout|Network|RemoteProtocol/.test(name)) return new TransientError(`${name}: ${message}`);
  return new PermanentError(`${name}: ${message}`);
}

function parseRetryAfter(value: string | undefined): number | null {
  if (!value) return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(0, n) : null;
}

/** Full-jitter delay for attempt (1-based): uniform(0, min(cap, base * mult^(n-1))). */
export function backoffCeiling(attempt: number, policy: RetryPolicy): number {
  if (attempt < 1) throw new Error("attempt is 1-based");
  return Math.min(policy.maxSeconds, policy.baseSeconds * policy.multiplier ** (attempt - 1));
}

export function backoffDelay(attempt: number, policy: RetryPolicy, rng: Rng): number {
  return rng.uniform(0, backoffCeiling(attempt, policy));
}

export interface RetryAttempt {
  attempt: number;
  outcome: "ok" | "transient" | "permanent" | "exhausted";
  status: number | null;
  error: string | null;
  /** Delay slept before the next attempt, when there is one. */
  delay: number | null;
  ceiling: number | null;
  retryAfter: number | null;
}

export interface RetryOutcome {
  attempts: number;
  delays: number[];
  trace: RetryAttempt[];
}

/**
 * Call fn until it succeeds, raises a PermanentError, or exhausts attempts.
 * Only TransientError triggers a retry; the sleep is virtual.
 */
export async function retryCall<T>(
  fn: () => Promise<T>,
  policy: RetryPolicy,
  rng: Rng,
  sleep: (seconds: number) => void,
  onRetry?: (attempt: number, delay: number, err: TransientError) => void,
): Promise<{ value: T; outcome: RetryOutcome }> {
  const outcome: RetryOutcome = { attempts: 0, delays: [], trace: [] };
  for (;;) {
    outcome.attempts += 1;
    try {
      const value = await fn();
      outcome.trace.push({ attempt: outcome.attempts, outcome: "ok", status: 200, error: null, delay: null, ceiling: null, retryAfter: null });
      return { value, outcome };
    } catch (exc) {
      const err = classifyException(exc);
      if (!err.retryable || outcome.attempts >= policy.maxAttempts) {
        outcome.trace.push({
          attempt: outcome.attempts,
          outcome: err.retryable ? "exhausted" : "permanent",
          status: err.status,
          error: err.message,
          delay: null,
          ceiling: null,
          retryAfter: null,
        });
        (err as DeliveryError & { outcome?: RetryOutcome }).outcome = outcome;
        throw err;
      }
      const transient = err as TransientError;
      const ceiling = backoffCeiling(outcome.attempts, policy);
      let delay = rng.uniform(0, ceiling);
      if (transient.retryAfter !== null) delay = Math.min(policy.maxSeconds, Math.max(delay, transient.retryAfter));
      outcome.delays.push(delay);
      outcome.trace.push({
        attempt: outcome.attempts,
        outcome: "transient",
        status: err.status,
        error: err.message,
        delay,
        ceiling,
        retryAfter: transient.retryAfter,
      });
      onRetry?.(outcome.attempts, delay, transient);
      sleep(delay);
    }
  }
}
