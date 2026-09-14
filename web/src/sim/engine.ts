// The browser stand-in for deploy/docker-compose.yml + demo/run.py: LocalStack
// queues, one worker per connector, three fakes, and the scenario the README reports.

import { buildAdapter, FakeTarget, type Adapter } from "./adapters";
import { idempotencyKey, IdempotencyStore } from "./idempotency";
import { makeTask, type Envelope, type Task } from "./models";
import { Clock, Rng } from "./prng";
import { listDeadLetters, Queue, replayDeadLetters, type QueueMessage } from "./queue";
import { SCHEMAS } from "./schema";
import { dlqName, loadAll, quarantineName, queueName, type ConnectorSpec } from "./specs";
import { percentile, Worker, type HandleTrace, type LogEvent } from "./worker";

export const UNIQUE_PER_CONNECTOR = 80;
export const DUPLICATES_PER_CONNECTOR = 20;
export const JIRA_RATE_LIMITED_TASKS = 30;
export const JIRA_429_ATTEMPTS = 2;
export const WEBHOOK_HARD_FAIL_TASKS = 10;

export interface ConnectorRuntime {
  spec: ConnectorSpec;
  queue: Queue;
  dlq: Queue;
  quarantine: Queue;
  target: FakeTarget;
  adapter: Adapter;
  worker: Worker;
}

export interface Summary {
  runId: string;
  submitted: number;
  unique: number;
  duplicates: number;
  deduplicated: number;
  delivered: Record<string, number>;
  uniqueKeys: Record<string, number>;
  dedupPer: Record<string, number>;
  retried: number;
  rejected429: number;
  rateLimitedTasks: number;
  byAttempt: Record<number, number>;
  delayMin: number;
  delayMedian: number;
  delayMax: number;
  deadLettered: number;
  deadLetterIds: string[];
  replayed: number;
  dlqAfterReplay: number;
  webhookDeliveredAfter: number;
  p50Ms: number;
  p95Ms: number;
  e2eP50: number;
  e2eP95: number;
  drainSeconds: number;
  ok: boolean;
  problems: string[];
}

export class Engine {
  readonly clock = new Clock();
  readonly rng: Rng;
  readonly runId: string;
  readonly specs: Record<string, ConnectorSpec>;
  readonly store: IdempotencyStore;
  readonly connectors: Record<string, ConnectorRuntime> = {};
  readonly log: LogEvent[] = [];
  private seq = 0;
  listeners = new Set<() => void>();

  constructor(seed = "D0D3904", specs?: Record<string, ConnectorSpec>) {
    this.runId = seed;
    this.rng = new Rng(seed);
    this.specs = specs ?? loadAll();
    const now = () => this.clock.now();
    this.store = new IdempotencyStore(now);
    for (const spec of Object.values(this.specs)) {
      const dlq = new Queue(dlqName(spec), now, 30, `${spec.name}-dlq`);
      const queue = new Queue(queueName(spec), now, spec.queue.visibilityTimeoutSeconds, spec.name);
      queue.configureRedrive(dlq, spec.queue.maxReceiveCount);
      const quarantine = new Queue(quarantineName(spec), now, 30, `${spec.name}-quarantine`);
      const target = new FakeTarget(spec.type, this.rng, now);
      const adapter = buildAdapter(spec, target, now);
      const worker = new Worker(spec, adapter, this.store, queue, this.clock, this.rng, (e) => this.emit({ ...e, connector: spec.name }), quarantine, SCHEMAS[spec.name] ?? null);
      this.connectors[spec.name] = { spec, queue, dlq, quarantine, target, adapter, worker };
    }
  }

  emit(e: Omit<LogEvent, "seq" | "t">): void {
    this.log.push({ ...e, seq: ++this.seq, t: this.clock.now() });
    if (this.log.length > 2000) this.log.splice(0, this.log.length - 2000);
  }

  notify(): void {
    for (const l of this.listeners) l();
  }

  async envelope(connector: string, task: Task): Promise<Envelope> {
    return { task, connector, idempotencyKey: await idempotencyKey(connector, task.id, task.version), submittedAt: this.clock.now(), attempt: 0 };
  }

  async submit(connector: string, tasks: Task[]): Promise<Envelope[]> {
    const envelopes: Envelope[] = [];
    for (const t of tasks) envelopes.push(await this.envelope(connector, t));
    this.connectors[connector].queue.sendBatch(envelopes);
    return envelopes;
  }

  /** Synthetic tasks: 80 unique per connector, mirroring demo/run.py. */
  syntheticTasks(name: string, count = UNIQUE_PER_CONNECTOR): Task[] {
    const tasks: Task[] = [];
    for (let i = 1; i <= count; i++) {
      tasks.push(
        makeTask({
          id: `${this.runId}-${name}-${String(i).padStart(4, "0")}`,
          title: `Synthetic task ${i} for ${name}`,
          body: `Body of task ${i}`,
          priority: this.rng.choice(["low", "normal", "high"]),
          labels: [name.split("-")[0], "demo"],
        }),
      );
    }
    return tasks;
  }

  /** Advance dt seconds: each worker handles up to requests_per_second * dt messages (at least one). */
  async tick(dt = 0.1): Promise<HandleTrace[]> {
    const traces: HandleTrace[] = [];
    for (const rt of Object.values(this.connectors)) {
      const budget = Math.max(1, Math.min(10, Math.round(rt.worker.requestsPerSecond * dt)));
      traces.push(...(await rt.worker.poll(budget)));
    }
    this.clock.advance(dt);
    return traces;
  }

  totalQueued(): number {
    return Object.values(this.connectors).reduce((n, rt) => n + rt.queue.messages.length, 0);
  }

  async drain(maxPolls = 10_000): Promise<number> {
    const started = this.clock.now();
    let idle = 0;
    for (let i = 0; i < maxPolls && idle < 3; i++) {
      const traces = await this.tick();
      if (!traces.length) idle += 1;
      else idle = 0;
    }
    return this.clock.now() - started;
  }

  deadLetters(name: string): QueueMessage[] {
    return listDeadLetters(this.connectors[name].dlq);
  }

  replay(name: string): QueueMessage[] {
    const rt = this.connectors[name];
    const moved = replayDeadLetters(rt.dlq, rt.queue);
    for (const m of moved) this.emit({ connector: name, kind: "replay", event: "dlq.replay", taskId: m.envelope.task.id, key: m.envelope.idempotencyKey.slice(0, 12), detail: `attempt=${m.envelope.attempt + 1} moved back to ${rt.queue.name}` });
    return moved;
  }

  reset(): void {
    this.clock.reset();
    this.store.reset();
    this.log.length = 0;
    this.seq = 0;
    for (const rt of Object.values(this.connectors)) {
      rt.queue.purge();
      rt.dlq.purge();
      rt.quarantine.purge();
      rt.target.clearInbox();
      rt.target.clearFaults();
      rt.worker.reset();
    }
  }
}

export interface ScenarioSetup {
  tasks: Record<string, Task[]>;
  rateLimited: string[];
  hardFailed: string[];
  duplicates: number;
  submitted: number;
}

/** Faults on, 300 messages queued: the first half of demo/run.py. */
export async function setupScenario(engine: Engine): Promise<ScenarioSetup> {
  const tasks: Record<string, Task[]> = {};
  for (const name of Object.keys(engine.specs)) tasks[name] = engine.syntheticTasks(name);
  const rateLimited = tasks["jira-support"].slice(0, JIRA_RATE_LIMITED_TASKS).map((t) => t.id);
  const hardFailed = tasks["webhook-crm"].slice(0, WEBHOOK_HARD_FAIL_TASKS).map((t) => t.id);
  engine.connectors["jira-support"].target.setFaults({ rateLimitTasks: new Set(rateLimited), rateLimitCount: JIRA_429_ATTEMPTS });
  engine.connectors["webhook-crm"].target.setFaults({ hardFailTasks: new Set(hardFailed) });
  let submitted = 0;
  let duplicates = 0;
  for (const name of Object.keys(engine.specs)) {
    const unique = tasks[name];
    const healthy = unique.filter((t) => !hardFailed.includes(t.id));
    const resubmits = engine.rng.sample(healthy, DUPLICATES_PER_CONNECTOR);
    const batch = engine.rng.shuffle([...unique, ...resubmits]);
    const sent = (await engine.submit(name, batch)).length;
    submitted += sent;
    duplicates += resubmits.length;
    engine.emit({ connector: name, kind: "submit", event: "submit", taskId: "", key: "", detail: `${sent} messages (${unique.length} unique, ${resubmits.length} resubmits)` });
  }
  return { tasks, rateLimited, hardFailed, duplicates, submitted };
}

/** Everything read off the queues and inboxes before the fault is cleared. */
export interface PhaseOne {
  drainSeconds: number;
  delivered: Record<string, number>;
  uniqueKeys: Record<string, number>;
  dedupPer: Record<string, number>;
  deadLetterIds: string[];
  byAttempt: Record<number, number>;
  delays: number[];
  latencies: number[];
  e2e: number[];
}

/** Snapshot the run at the point the README calls "queues drained". */
export function capturePhaseOne(engine: Engine, drainSeconds: number): PhaseOne {
  const delivered: Record<string, number> = {};
  const uniqueKeys: Record<string, number> = {};
  const dedupPer: Record<string, number> = {};
  for (const name of Object.keys(engine.specs)) {
    const rt = engine.connectors[name];
    delivered[name] = rt.target.inbox.length;
    uniqueKeys[name] = rt.target.seenKeys().size;
    dedupPer[name] = rt.worker.stats.deduplicated;
  }
  const byAttempt: Record<number, number> = {};
  for (const a of engine.connectors["jira-support"].worker.stats.retryAttempts) byAttempt[a] = (byAttempt[a] ?? 0) + 1;
  return {
    drainSeconds,
    delivered,
    uniqueKeys,
    dedupPer,
    deadLetterIds: engine.deadLetters("webhook-crm").map((m) => m.envelope.task.id).sort(),
    byAttempt,
    delays: Object.values(engine.connectors).flatMap((rt) => rt.worker.stats.retryDelays),
    latencies: Object.values(engine.connectors).flatMap((rt) => rt.worker.stats.latencies),
    e2e: Object.values(engine.connectors).flatMap((rt) => rt.target.inbox.map((e) => e.receivedAt)),
  };
}

/** Fold the two phases into the summary the README prints, with its checks applied. */
export function summarize(engine: Engine, setup: ScenarioSetup, one: PhaseOne, replayed: number): Summary {
  const jira = engine.connectors["jira-support"];
  const dlqAfter = engine.deadLetters("webhook-crm").length;
  const webhookAfter = engine.connectors["webhook-crm"].target.inbox.length;
  const deduplicated = Object.values(one.dedupPer).reduce((a, b) => a + b, 0);
  const deadLettered = one.deadLetterIds.length;
  const problems: string[] = [];
  if (deduplicated !== setup.duplicates) problems.push(`deduplicated ${deduplicated} != duplicates ${setup.duplicates}`);
  if (deadLettered !== WEBHOOK_HARD_FAIL_TASKS) problems.push(`dead letters ${deadLettered} != hard failures ${WEBHOOK_HARD_FAIL_TASKS}`);
  if (dlqAfter !== 0 || webhookAfter !== UNIQUE_PER_CONNECTOR) problems.push("replay did not drain the DLQ");
  for (const n of ["slack-ops", "jira-support"]) if (one.delivered[n] !== UNIQUE_PER_CONNECTOR) problems.push(`delivered ${n}=${one.delivered[n]}`);

  return {
    runId: engine.runId,
    submitted: setup.submitted,
    unique: Object.values(setup.tasks).reduce((n, t) => n + t.length, 0),
    duplicates: setup.duplicates,
    deduplicated,
    delivered: one.delivered,
    uniqueKeys: one.uniqueKeys,
    dedupPer: one.dedupPer,
    retried: Object.values(engine.connectors).reduce((n, rt) => n + rt.worker.stats.retried, 0),
    rejected429: jira.target.rejected,
    rateLimitedTasks: jira.target.rateLimitHits.size,
    byAttempt: one.byAttempt,
    delayMin: one.delays.length ? Math.min(...one.delays) : 0,
    delayMedian: percentile(one.delays, 50),
    delayMax: one.delays.length ? Math.max(...one.delays) : 0,
    deadLettered,
    deadLetterIds: one.deadLetterIds,
    replayed,
    dlqAfterReplay: dlqAfter,
    webhookDeliveredAfter: webhookAfter,
    p50Ms: percentile(one.latencies, 50) * 1000,
    p95Ms: percentile(one.latencies, 95) * 1000,
    e2eP50: percentile(one.e2e, 50),
    e2eP95: percentile(one.e2e, 95),
    drainSeconds: one.drainSeconds,
    ok: problems.length === 0,
    problems,
  };
}

/** The whole demo end to end, with the README's checks. */
export async function runScenario(engine: Engine): Promise<Summary> {
  engine.reset();
  const setup = await setupScenario(engine);
  const one = capturePhaseOne(engine, await engine.drain());
  engine.connectors["webhook-crm"].target.clearFaults();
  const replayed = engine.replay("webhook-crm").length;
  await engine.drain();
  return summarize(engine, setup, one, replayed);
}

const pad = (label: string) => label.padEnd(22);

/** The `conduit demo summary` block, in the layout demo/run.py prints it. */
export function formatSummary(s: Summary, planLines: string[], planSummary: string): string {
  const inboxes: Record<string, string> = { "jira-support": "jira fake inbox", "slack-ops": "slack fake inbox", "webhook-crm": "webhook fake inbox" };
  const lines = [
    `conduit demo summary (run ${s.runId}, browser port of demo/run.py)`,
    "=".repeat(72),
    `${pad("tasks submitted")}${s.submitted}  (${s.unique} unique + ${s.duplicates} duplicate resubmits)`,
    `${pad("deduplicated")}${s.deduplicated}  (must equal duplicates: ${s.deduplicated === s.duplicates ? "ok" : "MISMATCH"})`,
    "delivered per connector",
    ...Object.keys(s.delivered).map((n) => `  ${n.padEnd(18)}${String(s.delivered[n]).padStart(3)} delivered, ${String(s.uniqueKeys[n]).padStart(3)} unique keys, ${String(s.dedupPer[n]).padStart(3)} deduplicated (${inboxes[n] ?? "fake inbox"})`),
    `${pad("retried")}${s.retried}  (jira fake returned 429 ${s.rejected429} times for ${s.rateLimitedTasks} tasks)`,
    `${pad("  backoff evidence")}attempt 1 -> ${s.byAttempt[1] ?? 0} retries, attempt 2 -> ${s.byAttempt[2] ?? 0} retries; delay min/median/max ${s.delayMin.toFixed(3)}s / ${s.delayMedian.toFixed(3)}s / ${s.delayMax.toFixed(3)}s (policy base 0.25s x2, cap 8s, full jitter)`,
    `${pad("dead-lettered")}${s.deadLettered}  (must equal hard failures ${WEBHOOK_HARD_FAIL_TASKS}: ${s.deadLettered === WEBHOOK_HARD_FAIL_TASKS ? "ok" : "MISMATCH"}); conduit-webhook-crm-dlq after maxReceiveCount=2`,
    `${pad("  dead letters")}${s.deadLetterIds.map((t) => t.slice(-4)).join(", ")}`,
    `${pad("DLQ replay")}${s.replayed} replayed after clearing the fault; DLQ now ${s.dlqAfterReplay}; webhook delivered ${s.webhookDeliveredAfter}/${UNIQUE_PER_CONNECTOR}`,
    `${pad("delivery latency")}p50 ${s.p50Ms.toFixed(1)} ms, p95 ${s.p95Ms.toFixed(1)} ms (worker attempt-to-ack)`,
    `${pad("end-to-end latency")}p50 ${s.e2eP50.toFixed(2)} s, p95 ${s.e2eP95.toFixed(2)} s (submit-to-remote-receipt); queues drained in ${s.drainSeconds.toFixed(1)}s`,
    "new integration from one file (connectors/pager-oncall.yaml, 10 lines):",
    `  ${planSummary}`,
    ...planLines.map((a) => `  + ${a}`),
    "=".repeat(72),
    s.ok ? "all checks passed" : `CHECKS FAILED: ${s.problems.join("; ")}`,
  ];
  return lines.join("\n");
}
