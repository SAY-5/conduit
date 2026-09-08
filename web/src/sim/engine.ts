// The browser stand-in for deploy/docker-compose.yml + demo/run.py: LocalStack
// queues, one worker per connector, three fakes, and the scenario the README reports.

import { buildAdapter, FakeTarget, type Adapter } from "./adapters";
import { idempotencyKey, IdempotencyStore } from "./idempotency";
import { makeTask, type Envelope, type Task } from "./models";
import { Clock, Rng } from "./prng";
import { listDeadLetters, Queue, replayDeadLetters, type QueueMessage } from "./queue";
import { dlqName, loadAll, queueName, type ConnectorSpec } from "./specs";
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
      const target = new FakeTarget(spec.type, this.rng, now);
      const adapter = buildAdapter(spec, target, now);
      const worker = new Worker(spec, adapter, this.store, queue, this.clock, this.rng, (e) => this.emit({ ...e, connector: spec.name }));
      this.connectors[spec.name] = { spec, queue, dlq, target, adapter, worker };
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

/** The whole demo end to end, with the README's checks. */
export async function runScenario(engine: Engine): Promise<Summary> {
  engine.reset();
  const setup = await setupScenario(engine);
  const drainSeconds = await engine.drain();
  const specs = engine.specs;
  const delivered: Record<string, number> = {};
  const uniqueKeys: Record<string, number> = {};
  const dedupPer: Record<string, number> = {};
  for (const name of Object.keys(specs)) {
    const rt = engine.connectors[name];
    delivered[name] = rt.target.inbox.length;
    uniqueKeys[name] = rt.target.seenKeys().size;
    dedupPer[name] = rt.worker.stats.deduplicated;
  }
  const dead = engine.deadLetters("webhook-crm").map((m) => m.envelope.task.id).sort();
  const jira = engine.connectors["jira-support"];
  const byAttempt: Record<number, number> = {};
  for (const a of jira.worker.stats.retryAttempts) byAttempt[a] = (byAttempt[a] ?? 0) + 1;
  const allDelays = Object.values(engine.connectors).flatMap((rt) => rt.worker.stats.retryDelays);
  const allLatency = Object.values(engine.connectors).flatMap((rt) => rt.worker.stats.latencies);
  const e2e = Object.values(engine.connectors).flatMap((rt) => rt.target.inbox.map((e) => e.receivedAt));

  engine.connectors["webhook-crm"].target.clearFaults();
  const replayed = engine.replay("webhook-crm").length;
  await engine.drain();
  const dlqAfter = engine.deadLetters("webhook-crm").length;
  const webhookAfter = engine.connectors["webhook-crm"].target.inbox.length;

  const deduplicated = Object.values(dedupPer).reduce((a, b) => a + b, 0);
  const deadLettered = dead.length;
  const problems: string[] = [];
  if (deduplicated !== setup.duplicates) problems.push(`deduplicated ${deduplicated} != duplicates ${setup.duplicates}`);
  if (deadLettered !== WEBHOOK_HARD_FAIL_TASKS) problems.push(`dead letters ${deadLettered} != hard failures ${WEBHOOK_HARD_FAIL_TASKS}`);
  if (dlqAfter !== 0 || webhookAfter !== UNIQUE_PER_CONNECTOR) problems.push("replay did not drain the DLQ");
  for (const n of ["slack-ops", "jira-support"]) if (delivered[n] !== UNIQUE_PER_CONNECTOR) problems.push(`delivered ${n}=${delivered[n]}`);

  return {
    runId: engine.runId,
    submitted: setup.submitted,
    unique: Object.values(setup.tasks).reduce((n, t) => n + t.length, 0),
    duplicates: setup.duplicates,
    deduplicated,
    delivered,
    uniqueKeys,
    dedupPer,
    retried: Object.values(engine.connectors).reduce((n, rt) => n + rt.worker.stats.retried, 0),
    rejected429: jira.target.rejected,
    rateLimitedTasks: jira.target.rateLimitHits.size,
    byAttempt,
    delayMin: allDelays.length ? Math.min(...allDelays) : 0,
    delayMedian: percentile(allDelays, 50),
    delayMax: allDelays.length ? Math.max(...allDelays) : 0,
    deadLettered,
    deadLetterIds: dead,
    replayed,
    dlqAfterReplay: dlqAfter,
    webhookDeliveredAfter: webhookAfter,
    p50Ms: percentile(allLatency, 50) * 1000,
    p95Ms: percentile(allLatency, 95) * 1000,
    e2eP50: percentile(e2e, 50),
    e2eP95: percentile(e2e, 95),
    drainSeconds,
    ok: problems.length === 0,
    problems,
  };
}
