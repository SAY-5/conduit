// Port of conduit/worker.py: SQS in, adapter out, idempotency and retry in between.

import type { Adapter } from "./adapters";
import { isDuplicate, type IdempotencyStore } from "./idempotency";
import type { DeliveryResult } from "./models";
import type { Clock, Rng } from "./prng";
import type { Queue, QueueMessage } from "./queue";
import { DeliveryError, retryCall, TransientError, type RetryOutcome } from "./retry";
import { validatePayload, type SourceSchema } from "./schema";
import type { ConnectorSpec } from "./specs";
import { CircuitBreaker, isTargetFailure, TokenBucket } from "./throttle";

export const IN_PROGRESS_RECHECK_SECONDS = 10;
/** While the breaker is open the worker sleeps in slices this long. */
export const BREAKER_PAUSE_SLICE = 1;

export type EventKind =
  | "claim"
  | "ok"
  | "dedup"
  | "retry"
  | "fail"
  | "dlq"
  | "quarantine"
  | "replay"
  | "in_progress"
  | "submit"
  | "info"
  | "throttle"
  | "breaker";

export interface LogEvent {
  seq: number;
  t: number;
  connector: string;
  kind: EventKind;
  event: string;
  taskId: string;
  key: string;
  detail: string;
  data?: Record<string, unknown>;
}

export interface WorkerStats {
  received: number;
  delivered: number;
  deduplicated: number;
  quarantined: number;
  retried: number;
  failed: number;
  failedPermanent: number;
  failedExhausted: number;
  deadLettered: number;
  latencies: number[];
  retryDelays: number[];
  retryAttempts: number[];
  queueLags: number[];
  rateLimitWaits: number;
  rateLimitWaitSeconds: number;
  retryAfterHonored: number;
  breakerOpens: number;
  breakerPausedSeconds: number;
  breakerHolds: number;
}

export function emptyStats(): WorkerStats {
  return {
    received: 0,
    delivered: 0,
    deduplicated: 0,
    quarantined: 0,
    retried: 0,
    failed: 0,
    failedPermanent: 0,
    failedExhausted: 0,
    deadLettered: 0,
    latencies: [],
    retryDelays: [],
    retryAttempts: [],
    queueLags: [],
    rateLimitWaits: 0,
    rateLimitWaitSeconds: 0,
    retryAfterHonored: 0,
    breakerOpens: 0,
    breakerPausedSeconds: 0,
    breakerHolds: 0,
  };
}

export interface HandleTrace {
  message: QueueMessage;
  claim: ReturnType<IdempotencyStore["claim"]>;
  result: DeliveryResult | null;
  retry: RetryOutcome | null;
  elapsed: number;
  willDeadLetter: boolean;
  reason: "delivered" | "deduplicated" | "quarantined" | "permanent" | "exhausted" | "in_progress" | "breaker_open";
}

export class Worker {
  readonly stats = emptyStats();
  /** Messages this worker may handle per second: the spec's rate limit. */
  readonly requestsPerSecond: number;
  /** Paces sends at the connector's rate limit and absorbs Retry-After pauses. */
  readonly bucket: TokenBucket;
  /** Pauses the connector while its target looks down. */
  readonly breaker: CircuitBreaker;

  constructor(
    readonly spec: ConnectorSpec,
    readonly adapter: Adapter,
    readonly store: IdempotencyStore,
    readonly queue: Queue,
    readonly clock: Clock,
    readonly rng: Rng,
    readonly log: (e: Omit<LogEvent, "seq" | "t" | "connector">) => void,
    readonly quarantine: Queue | null = null,
    readonly schema: SourceSchema | null = null,
  ) {
    this.requestsPerSecond = spec.rateLimit.requestsPerSecond;
    const now = () => this.clock.now();
    this.bucket = new TokenBucket(spec.rateLimit.requestsPerSecond, spec.rateLimit.burst, spec.rateLimit.maxRetryAfterSeconds, now);
    this.breaker = new CircuitBreaker(spec.breaker.failureThreshold, spec.breaker.recoverySeconds, now);
  }

  /** One poll: receive a batch, handle each message. Returns the traces. */
  async poll(max = 10): Promise<HandleTrace[]> {
    if (this.isOpen()) {
      this.pauseWhileOpen([]);
      return [];
    }
    const batch = this.queue.receive(max);
    const traces: HandleTrace[] = [];
    for (let i = 0; i < batch.length; i++) {
      if (this.isOpen()) {
        this.pauseWhileOpen(batch.slice(i));
        break;
      }
      traces.push(await this.handle(batch[i]));
    }
    return traces;
  }

  private isOpen(): boolean {
    return this.breaker.state === "open";
  }

  /** Reserve a token; sleep for however long the bucket says the send must wait. */
  private throttle(): void {
    const wait = this.bucket.acquire();
    if (wait <= 0) return;
    this.stats.rateLimitWaits += 1;
    this.stats.rateLimitWaitSeconds += wait;
    this.clock.advance(wait);
  }

  /** Feed one transient attempt to the bucket (429) or the breaker (target down). */
  private noteTransient(err: TransientError, taskId: string, key: string): void {
    if (err.status === 429 && err.retryAfter !== null) {
      const pause = this.bucket.penalize(err.retryAfter);
      this.stats.retryAfterHonored += 1;
      this.log({ kind: "throttle", event: "rate_limit.retry_after", taskId, key, detail: `pause=${pause.toFixed(3)}s for every send on this connector` });
    }
    if (isTargetFailure(err.status) && this.breaker.recordFailure()) {
      this.stats.breakerOpens += 1;
      this.log({ kind: "breaker", event: "breaker.open", taskId, key, detail: `threshold=${this.breaker.failureThreshold} recovery=${this.breaker.recoverySeconds}s` });
    }
  }

  private noteTargetOk(taskId: string, key: string): void {
    if (this.breaker.recordSuccess()) {
      this.log({ kind: "breaker", event: "breaker.closed", taskId, key, detail: "probe succeeded" });
    }
  }

  /** Sleep until the breaker half-opens, keeping any held messages invisible. */
  private pauseWhileOpen(held: QueueMessage[]): void {
    this.log({ kind: "breaker", event: "breaker.paused", taskId: "", key: "", detail: `remaining=${this.breaker.remaining().toFixed(3)}s held=${held.length}` });
    let guard = 0;
    while (this.isOpen() && guard++ < 10_000) {
      const remaining = this.breaker.remaining();
      for (const m of held) this.queue.changeVisibility(m.messageId, Math.floor(remaining) + 5);
      const nap = Math.min(BREAKER_PAUSE_SLICE, remaining);
      this.clock.advance(nap);
      this.stats.breakerPausedSeconds += nap;
      if (nap <= 0) break;
    }
    if (this.breaker.state === "half_open") {
      this.log({ kind: "breaker", event: "breaker.half_open", taskId: "", key: "", detail: "one probe admitted" });
    }
  }

  async handle(message: QueueMessage): Promise<HandleTrace> {
    const env = message.envelope;
    const task = env.task;
    const key = env.idempotencyKey;
    const name = this.spec.name;
    const s = this.stats;
    s.received += 1;
    s.queueLags.push(Math.max(0, this.clock.now() - env.submittedAt));
    const short = key.slice(0, 12);

    const note = this.schema ? validatePayload(this.schema, task) : null;
    if (note) {
      this.quarantine?.send({ ...env, quarantine: note });
      this.queue.delete(message.messageId);
      s.quarantined += 1;
      this.log({ kind: "quarantine", event: "delivery.quarantined", taskId: task.id, key: short, detail: `stage=${note.stage} field=${note.field} reason=${note.reason}` });
      const result: DeliveryResult = { status: "quarantined", connector: name, taskId: task.id, idempotencyKey: key, remoteId: null, attempts: message.receiveCount, detail: `${note.stage} ${note.reason}: ${note.detail}` };
      return { message, claim: { acquired: false, state: null, remoteId: null, condition: "failed" }, result, retry: null, elapsed: 0, willDeadLetter: false, reason: "quarantined" };
    }

    if (!this.breaker.allow()) {
      const hold = Math.floor(this.breaker.remaining()) + 1;
      this.queue.changeVisibility(message.messageId, hold);
      this.stats.breakerHolds += 1;
      this.log({ kind: "breaker", event: "delivery.breaker_open", taskId: task.id, key: short, detail: `hold=${hold}s` });
      return { message, claim: { acquired: false, state: null, remoteId: null, condition: "failed" }, result: null, retry: null, elapsed: 0, willDeadLetter: false, reason: "breaker_open" };
    }

    const claim = this.store.claim(key, name, task.id);
    if (!claim.acquired) {
      if (isDuplicate(claim)) {
        this.queue.delete(message.messageId);
        s.deduplicated += 1;
        this.log({ kind: "dedup", event: "delivery.deduplicated", taskId: task.id, key: short, detail: `remote_id=${claim.remoteId ?? "none"}` });
        const result: DeliveryResult = { status: "deduplicated", connector: name, taskId: task.id, idempotencyKey: key, remoteId: claim.remoteId, attempts: 1, detail: "idempotency key already delivered" };
        return { message, claim, result, retry: null, elapsed: 0, willDeadLetter: false, reason: "deduplicated" };
      }
      this.queue.changeVisibility(message.messageId, IN_PROGRESS_RECHECK_SECONDS);
      this.log({ kind: "in_progress", event: "delivery.in_progress_elsewhere", taskId: task.id, key: short, detail: `recheck=${IN_PROGRESS_RECHECK_SECONDS}s` });
      return { message, claim, result: null, retry: null, elapsed: 0, willDeadLetter: false, reason: "in_progress" };
    }
    this.log({ kind: "claim", event: "claim.acquired", taskId: task.id, key: short, detail: claim.condition });

    const remoteId = task.version > 1 ? this.store.latest(name, task.id) : null;
    this.throttle();
    const started = this.clock.now();
    const sleep = (seconds: number) => this.clock.advance(seconds);

    let retry: RetryOutcome | null = null;
    try {
      const { value, outcome } = await retryCall(
        () => this.adapter.deliver(task, key, remoteId, env.attempt > 0),
        this.spec.retry,
        this.rng,
        sleep,
        (attempt, delay, err) => {
          s.retried += 1;
          s.retryDelays.push(delay);
          s.retryAttempts.push(attempt);
          const budget = Math.floor(delay + this.spec.retry.timeoutSeconds) + 5;
          this.queue.changeVisibility(message.messageId, budget);
          this.log({ kind: "retry", event: "delivery.retry", taskId: task.id, key: short, detail: `attempt=${attempt} delay=${delay.toFixed(3)}s ${err.message.slice(0, 40)}`, data: { attempt, delay, status: err.status } });
          this.noteTransient(err, task.id, short);
        },
      );
      retry = outcome;
      this.noteTargetOk(task.id, short);
      this.clock.advance(this.adapter.target.lastLatency);
      const elapsed = this.clock.now() - started;
      this.store.markDelivered(key, value.remoteId);
      this.queue.delete(message.messageId);
      s.delivered += 1;
      s.latencies.push(elapsed);
      this.log({ kind: "ok", event: "delivery.ok", taskId: task.id, key: short, detail: `remote_id=${value.remoteId ?? "none"} attempts=${outcome.attempts} elapsed=${(elapsed * 1000).toFixed(1)}ms` });
      return { message, claim, result: { ...value, attempts: outcome.attempts }, retry, elapsed, willDeadLetter: false, reason: "delivered" };
    } catch (exc) {
      const err = exc as DeliveryError & { outcome?: RetryOutcome };
      retry = err.outcome ?? null;
      if (err instanceof TransientError) this.noteTransient(err, task.id, short);
      this.store.release(key);
      const elapsed = this.clock.now() - started;
      const reason = err.retryable ? "exhausted" : "permanent";
      s.failed += 1;
      if (reason === "permanent") s.failedPermanent += 1;
      else s.failedExhausted += 1;
      const willDeadLetter = message.receiveCount >= this.spec.queue.maxReceiveCount;
      if (willDeadLetter) s.deadLettered += 1;
      this.log({
        kind: willDeadLetter ? "dlq" : "fail",
        event: "delivery.failed",
        taskId: task.id,
        key: short,
        detail: `reason=${reason} receive_count=${message.receiveCount} dead_letter=${willDeadLetter} ${err.message.slice(0, 40)}`,
        data: { reason, receiveCount: message.receiveCount, willDeadLetter, status: err.status },
      });
      this.queue.changeVisibility(message.messageId, 0);
      const result: DeliveryResult = { status: "failed", connector: name, taskId: task.id, idempotencyKey: key, remoteId: null, attempts: message.receiveCount, detail: err.message };
      return { message, claim, result, retry, elapsed, willDeadLetter, reason };
    }
  }

  reset(): void {
    Object.assign(this.stats, emptyStats());
    this.bucket.reset();
    this.breaker.reset();
  }
}

export function percentile(values: number[], pct: number): number {
  if (!values.length) return 0;
  const ordered = values.slice().sort((a, b) => a - b);
  const rank = (pct / 100) * (ordered.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.min(lo + 1, ordered.length - 1);
  return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo);
}
