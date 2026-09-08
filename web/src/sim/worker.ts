// Port of conduit/worker.py: SQS in, adapter out, idempotency and retry in between.

import type { Adapter } from "./adapters";
import { isDuplicate, type IdempotencyStore } from "./idempotency";
import type { DeliveryResult } from "./models";
import type { Clock, Rng } from "./prng";
import type { Queue, QueueMessage } from "./queue";
import { DeliveryError, retryCall, type RetryOutcome } from "./retry";
import type { ConnectorSpec } from "./specs";

export const IN_PROGRESS_RECHECK_SECONDS = 10;

export type EventKind = "claim" | "ok" | "dedup" | "retry" | "fail" | "dlq" | "replay" | "in_progress" | "submit" | "info";

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
  retried: number;
  failed: number;
  failedPermanent: number;
  failedExhausted: number;
  deadLettered: number;
  latencies: number[];
  retryDelays: number[];
  retryAttempts: number[];
  queueLags: number[];
}

export function emptyStats(): WorkerStats {
  return { received: 0, delivered: 0, deduplicated: 0, retried: 0, failed: 0, failedPermanent: 0, failedExhausted: 0, deadLettered: 0, latencies: [], retryDelays: [], retryAttempts: [], queueLags: [] };
}

export interface HandleTrace {
  message: QueueMessage;
  claim: ReturnType<IdempotencyStore["claim"]>;
  result: DeliveryResult | null;
  retry: RetryOutcome | null;
  elapsed: number;
  willDeadLetter: boolean;
  reason: "delivered" | "deduplicated" | "permanent" | "exhausted" | "in_progress";
}

export class Worker {
  readonly stats = emptyStats();
  /** Messages this worker may handle per second: the spec's rate limit. */
  readonly requestsPerSecond: number;

  constructor(
    readonly spec: ConnectorSpec,
    readonly adapter: Adapter,
    readonly store: IdempotencyStore,
    readonly queue: Queue,
    readonly clock: Clock,
    readonly rng: Rng,
    readonly log: (e: Omit<LogEvent, "seq" | "t" | "connector">) => void,
  ) {
    this.requestsPerSecond = spec.rateLimit.requestsPerSecond;
  }

  /** One poll: receive a batch, handle each message. Returns the traces. */
  async poll(max = 10): Promise<HandleTrace[]> {
    const batch = this.queue.receive(max);
    const traces: HandleTrace[] = [];
    for (const message of batch) traces.push(await this.handle(message));
    return traces;
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
        },
      );
      retry = outcome;
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
