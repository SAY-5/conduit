// Console self-check: the README numbers, reproduced by the browser port, plus
// unit assertions on the pieces those numbers depend on.
// Run with `npm run selfcheck` (Node 20+, Web Crypto via globalThis.crypto).
//
// Every assertion line is built from terms. A term compares two operands and prints both of them
// with the relation that actually holds, so the text of a line is its verdict and cannot say
// anything the line did not check.

import { RATE_LIMIT_STATUS } from "./adapters";
import { Engine, JIRA_429_ATTEMPTS, JIRA_RATE_LIMITED_TASKS, runScenario, summarize, UNIQUE_PER_CONNECTOR, WEBHOOK_HARD_FAIL_TASKS, type PhaseOne, type ScenarioSetup, type Summary } from "./engine";
import { idempotencyKey, IdempotencyStore, keyMaterial, sha256Hex } from "./idempotency";
import { makeTask } from "./models";
import { Clock } from "./prng";
import { Queue } from "./queue";
import { backoffCeiling, classifyResponse, PermanentError, TransientError } from "./retry";
import { CONNECTOR_YAML, describeRule, loadSpec, NEW_CONNECTOR_YAML } from "./specs";
import { diffPlans, plan, TABLE_RESOURCE } from "./terraform";
import { CircuitBreaker, isTargetFailure, TokenBucket } from "./throttle";

export interface CheckLine {
  label: string;
  value: string;
  /** true/false for an assertion, null for a reported figure. */
  ok: boolean | null;
}

export interface CheckReport {
  lines: CheckLine[];
  ok: boolean;
  passed: number;
  failed: number;
  /** How many lines are assertions rather than reported figures. */
  assertions: number;
}

/**
 * A finished run to check. A caller that has just run the scenario on screen passes its own
 * engine and snapshots so the assertions describe what the viewer saw; Node passes nothing and
 * the check runs its own.
 */
export interface FinishedRun {
  engine: Engine;
  setup: ScenarioSetup;
  one: PhaseOne;
  summary?: Summary;
}

/* ---------------------------------------------------------------- terms */

interface Operand {
  value: unknown;
  /** What the value is, printed in front of it. */
  name: string;
  unit: string;
  /** A computed operand prints the operands it was computed from, then its value. */
  parts?: { of: Operand[]; joiner: string };
}

const val = (value: unknown, name = "", unit = ""): Operand => ({ value, name, unit });
const readme = (value: unknown, unit = ""): Operand => val(value, "README", unit);
const sum = (...of: Operand[]): Operand => ({ value: of.reduce((n, o) => n + Number(o.value), 0), name: "", unit: "", parts: { of, joiner: " + " } });
const product = (...of: Operand[]): Operand => ({ value: of.reduce((n, o) => n * Number(o.value), 1), name: "", unit: "", parts: { of, joiner: " x " } });

type Relation = "=" | "!=" | ">=" | "<=" | "~=" | "~";

/** How each relation reads when it holds, and when it does not. */
const READS: Record<Relation, [string, string]> = {
  "=": ["=", "!="],
  "!=": ["differs from", "=="],
  ">=": [">=", "<"],
  "<=": ["<=", ">"],
  "~=": ["~=", "!~="],
  "~": ["matches", "does not match"],
};

const same = (a: unknown, b: unknown) => Object.is(a, b) || (typeof a === "object" && a !== null && JSON.stringify(a) === JSON.stringify(b));

function holds(a: unknown, relation: Relation, b: unknown): boolean {
  switch (relation) {
    case "=":
      return same(a, b);
    case "!=":
      return !same(a, b);
    case ">=":
      return typeof a === "number" && typeof b === "number" && a >= b;
    case "<=":
      return typeof a === "number" && typeof b === "number" && a <= b;
    case "~=":
      return typeof a === "number" && typeof b === "number" && Math.abs(a - b) < 1e-9;
    case "~":
      return b instanceof RegExp && typeof a === "string" && b.test(a);
  }
}

const HEX_DIGEST = /^[0-9a-f]{64}$/;

/** A value as a line prints it; `full` when the term fails, so nothing that differs is abbreviated away. */
function show(value: unknown, full: boolean): string {
  if (typeof value === "number") return Number.isInteger(value) || full ? String(value) : value.toFixed(3);
  if (typeof value === "string") return HEX_DIGEST.test(value) && !full ? `${value.slice(0, 16)}...` : JSON.stringify(value);
  if (value instanceof RegExp) return String(value);
  if (Array.isArray(value)) return showList(value, full);
  return value === undefined ? "undefined" : JSON.stringify(value);
}

/** A list, with the prefix every item shares written once: D0D3904-webhook-crm-{0001,0002}. */
function showList(items: unknown[], full: boolean): string {
  if (items.length > 1 && items.every((item): item is string => typeof item === "string")) {
    let prefix = items[0];
    for (const item of items) while (!item.startsWith(prefix)) prefix = prefix.slice(0, -1);
    prefix = prefix.replace(/[A-Za-z0-9]+$/, "");
    if (prefix.length >= 8) return `${prefix}{${items.map((item) => item.slice(prefix.length)).join(",")}}`;
  }
  return `[${items.map((item) => show(item, full)).join(", ")}]`;
}

function operandText(o: Operand, full: boolean): string {
  const shown = `${show(o.value, full)}${o.unit}`;
  if (o.parts) return `${o.parts.of.map((p) => operandText(p, full)).join(o.parts.joiner)} (${shown})`;
  return o.name ? `${o.name} ${shown}` : shown;
}

interface Term {
  holds: boolean;
  text: string;
}

function term(a: Operand, relation: Relation, b: Operand): Term {
  const ok = holds(a.value, relation, b.value);
  const [yes, no] = READS[relation];
  return { holds: ok, text: `${operandText(a, !ok)} ${ok ? yes : no} ${operandText(b, !ok)}` };
}

/** An assertion line: it passes when every term holds, and it prints every term. */
function line(label: string, ...terms: Term[]): CheckLine {
  return { label, value: terms.map((t) => t.text).join("; "), ok: terms.length > 0 && terms.every((t) => t.holds) };
}

/** A figure the run reports and nothing asserts. */
const figure = (label: string, value: string): CheckLine => ({ label, value, ok: null });

/* --------------------------------------------------------- reproducibility */

interface NodeProcess {
  argv: string[];
  exitCode?: number;
  versions?: { node?: string };
  getBuiltinModule?: (id: string) => unknown;
}
const proc = (globalThis as { process?: NodeProcess }).process;
const inNode = typeof proc?.versions?.node === "string";

/** What a module in src/sim may not name: each is a source of randomness or of wall-clock time. */
const NONDETERMINISTIC = ["Math.random", "Date.now", "performance.now", "new Date"];

/** The source of every other module in src/sim, read from disk; null where there is no disk to read. */
function simSources(): Record<string, string> | null {
  const fs = proc?.getBuiltinModule?.("node:fs") as { readdirSync(dir: string): string[]; readFileSync(file: string, encoding: "utf8"): string } | undefined;
  const url = proc?.getBuiltinModule?.("node:url") as { fileURLToPath(u: string): string } | undefined;
  const path = proc?.getBuiltinModule?.("node:path") as { dirname(p: string): string; join(...parts: string[]): string } | undefined;
  if (!fs || !url || !path) return null;
  const dir = path.dirname(url.fileURLToPath(import.meta.url));
  const sources: Record<string, string> = {};
  for (const file of fs.readdirSync(dir).sort()) if (file.endsWith(".ts") && file !== "selfcheck.ts") sources[file] = fs.readFileSync(path.join(dir, file), "utf8");
  return sources;
}

/**
 * Every value reachable from the engine, as JSON: its clock, the rng's position, the claim store
 * (items, latest remote ids, events), the log and its sequence, and for each connector the three
 * queues (messages, redrives, message id sequence, counters), the fake (inbox, faults, fired
 * faults, rate-limit hits, counters) and the worker (stats, token bucket, breaker), with the specs
 * they were built from. Functions are left out, and so are the page's listeners, which belong to
 * the page rather than to the run. A value reached a second time is written as a marker.
 */
function stateOf(engine: Engine): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(engine, (key, value: unknown) => {
    if (typeof value === "function" || (key === "listeners" && value instanceof Set)) return undefined;
    if (value === null || typeof value !== "object") return value;
    if (seen.has(value)) return "(seen)";
    seen.add(value);
    if (value instanceof Map) return { map: [...value] };
    if (value instanceof Set) return { set: [...value] };
    return value;
  });
}

/** A finished run, hashed: its summary and the engine's whole state. */
const fingerprint = (engine: Engine, summary: Summary): Promise<string> => sha256Hex(`${JSON.stringify(summary)}\n${stateOf(engine)}`);

/**
 * Move every part of the engine that reset() restores away from where a new engine starts. A run
 * moves most of them, but it leaves the breakers closed, the queues empty and the one-shot 500
 * fault unused, so each connector also gets a tripped breaker, a paused bucket, a message on each
 * of its queues and a fired 500.
 */
async function disturb(engine: Engine): Promise<void> {
  for (const [name, rt] of Object.entries(engine.connectors)) {
    const task = makeTask({ id: `${name}-left-behind`, title: "left behind" });
    const envelope = await engine.envelope(name, task);
    for (const queue of [rt.queue, rt.dlq, rt.quarantine]) queue.send(envelope);
    for (let i = 0; i < rt.spec.breaker.failureThreshold; i++) rt.worker.breaker.recordFailure();
    rt.worker.bucket.penalize(1);
    rt.target.setFaults({ fail500Once: true });
    rt.target.handle({ method: "POST", url: rt.spec.target, headers: {}, body: {} }, task, false);
  }
}

/**
 * The run replayed from its seed must match it exactly: on a fresh engine and, in Node, on that
 * engine again after the reset "Run again" goes through. Node also disturbs that engine, resets it
 * and requires the state of a new one, and reads src/sim and requires that no module names a
 * source of randomness or wall-clock time.
 */
async function reproducibility(engine: Engine, summary: Summary, onScreen: boolean): Promise<Term[]> {
  const seed = engine.runId;
  const name = onScreen ? "the run on screen" : "this run";
  const printed = await fingerprint(engine, summary);
  await yieldToPage();
  const fresh = new Engine(seed);
  const first = await fingerprint(fresh, await runScenario(fresh));
  const terms = [term(val(printed, name), "=", val(first, `a fresh engine at seed ${seed}`))];
  if (!inNode) return terms;
  const again = await fingerprint(fresh, await runScenario(fresh));
  terms.push(term(val(printed, name), "=", val(again, "that engine run again")));
  await disturb(fresh);
  fresh.reset();
  terms.push(term(val(await sha256Hex(stateOf(fresh)), "that engine disturbed and reset"), "=", val(await sha256Hex(stateOf(new Engine(seed))), "a new engine")));
  const sources = simSources();
  if (!sources) return [...terms, term(val(false, "src/sim readable"), "=", val(true))];
  const files = Object.keys(sources);
  terms.push(term(val(files.length, "src/sim files read"), ">=", val(1)));
  for (const token of NONDETERMINISTIC) {
    const hits = files.filter((file) => sources[file].includes(token));
    terms.push(term(val(hits.length, hits.length ? `files naming ${token} (${hits.join(", ")})` : `files naming ${token}`), "=", val(0)));
  }
  return terms;
}

/* ---------------------------------------------------------------- scenario */

const SEED = "D0D3904";

/** The figures the README's demo block prints, which the scenario lines reproduce. */
const README = {
  submitted: 302,
  unique: 240,
  resubmits: 60,
  malformed: 2,
  deduplicated: 60,
  delivered: { "jira-support": 80, "slack-ops": 80, "webhook-crm": 70 } as Record<string, number>,
  deduplicatedPerConnector: 20,
  retried: 60,
  rateLimitedTasks: 30,
  retriesPerAttempt: 30,
  quarantined: 2,
  quarantineNotes: ["jira-support/schema: priority enum", "webhook-crm/schema: status enum"],
  deadLettered: 10,
  replayed: 10,
  planned: 8,
  shippedResources: 26,
  shippedConnectors: 3,
  jira: { burst: 5, failures: 5, recoverySeconds: 30, retryAfterCap: 120 },
};

/** A connector's longest retry delay after each attempt that was retried, in attempt order. */
function longestDelayByAttempt(engine: Engine, name: string): [number, number][] {
  const { retryAttempts, retryDelays } = engine.connectors[name].worker.stats;
  const longest = new Map<number, number>();
  retryAttempts.forEach((attempt, i) => longest.set(attempt, Math.max(longest.get(attempt) ?? 0, retryDelays[i])));
  return [...longest].sort((a, b) => a[0] - b[0]);
}

/** The scenario numbers the README prints, checked against the values it prints. */
async function scenarioLines(finished?: FinishedRun): Promise<CheckLine[]> {
  const engine = finished ? finished.engine : new Engine(SEED);
  const s = finished ? (finished.summary ?? summarize(engine, finished.setup, finished.one, finished.one.deadLetterIds.length)) : await runScenario(engine);
  const reproduced = await reproducibility(engine, s, finished !== undefined);
  const names = Object.keys(engine.specs);
  const jira = engine.connectors["jira-support"];
  const rateLimited = jira.target.responses.get(RATE_LIMIT_STATUS) ?? 0;
  const claimFailures = engine.store.events.filter((e) => e.outcome === "ConditionalCheckFailedException").length;
  const paced = Object.values(engine.connectors).reduce((n, rt) => n + rt.worker.stats.rateLimitWaits, 0);
  const deliveredAfter = Object.values(engine.connectors).reduce((n, rt) => n + rt.target.inbox.length, 0);
  const firstTasks = Array.from({ length: WEBHOOK_HARD_FAIL_TASKS }, (_, i) => `${engine.runId}-webhook-crm-${String(i + 1).padStart(4, "0")}`);
  const retriesAfter = (attempt: number) => s.byAttempt[attempt] ?? 0;
  return [
    line("tasks submitted",
      term(val(s.submitted, "submitted"), "=", readme(README.submitted)),
      term(sum(val(s.unique, "unique"), val(s.duplicates, "resubmits"), val(s.malformed, "malformed")), "=", val(s.submitted, "submitted")),
      term(val(s.malformed, "malformed"), "=", readme(README.malformed))),
    line("  unique tasks",
      term(val(s.unique, "unique"), "=", readme(README.unique)),
      term(product(val(names.length, "connectors"), val(UNIQUE_PER_CONNECTOR, "tasks each")), "=", val(s.unique, "unique"))),
    line("  resubmits", term(val(s.duplicates, "resubmits"), "=", readme(README.resubmits))),
    line("deduplicated",
      term(val(s.deduplicated, "deduplicated"), "=", val(s.duplicates, "resubmits")),
      term(val(s.deduplicated, "deduplicated"), "=", readme(README.deduplicated))),
    line("  conditional puts", term(val(claimFailures, "ConditionalCheckFailedException"), ">=", val(s.deduplicated, "deduplicated"))),
    ...names.map((n) =>
      line(`  ${n}`,
        term(val(s.delivered[n], "delivered"), "=", readme(README.delivered[n])),
        term(val(s.uniqueKeys[n], "unique keys"), "=", val(s.delivered[n], "delivered")),
        term(val(s.dedupPer[n], "deduplicated"), "=", readme(README.deduplicatedPerConnector))),
    ),
    line("  jira delivered", term(val(s.delivered["jira-support"], "delivered"), "=", val(UNIQUE_PER_CONNECTOR, "unique"))),
    line("  slack delivered", term(val(s.delivered["slack-ops"], "delivered"), "=", val(UNIQUE_PER_CONNECTOR, "unique"))),
    line("retried",
      term(val(s.retried, "retried"), "=", readme(README.retried)),
      term(val(s.retried, "retried"), "=", val(s.rejected429, "rejected attempts"))),
    line(`  ${RATE_LIMIT_STATUS} responses`,
      term(val(rateLimited, `status ${RATE_LIMIT_STATUS} from the jira fake`), "=", product(val(JIRA_RATE_LIMITED_TASKS, "tasks"), val(JIRA_429_ATTEMPTS, "attempts"))),
      term(val(s.rejected429, "rejections the summary counts"), "=", val(rateLimited, `status ${RATE_LIMIT_STATUS}`))),
    line("  rate-limited tasks",
      term(val(s.rateLimitedTasks, "tasks"), "=", val(JIRA_RATE_LIMITED_TASKS, "configured")),
      term(val(s.rateLimitedTasks, "tasks"), "=", readme(README.rateLimitedTasks))),
    line("  backoff evidence",
      term(val(retriesAfter(1), "retries after attempt 1"), "=", readme(README.retriesPerAttempt)),
      term(val(retriesAfter(2), "after attempt 2"), "=", readme(README.retriesPerAttempt))),
    figure("  retry delays", `min ${s.delayMin.toFixed(3)}s, median ${s.delayMedian.toFixed(3)}s, max ${s.delayMax.toFixed(3)}s`),
    line("  jitter within cap",
      term(val(s.delayMin, "shortest delay", "s"), ">=", val(0, "", "s")),
      ...names.flatMap((n) => longestDelayByAttempt(engine, n).map(([attempt, longest]) =>
        term(val(longest, `${n} after attempt ${attempt} longest`, "s"), "<=", val(backoffCeiling(attempt, engine.connectors[n].spec.retry), "its ceiling", "s"))))),
    line("  Retry-After honoured", term(val(jira.worker.stats.retryAfterHonored, "responses whose Retry-After reached the bucket"), "=", val(rateLimited, `status ${RATE_LIMIT_STATUS} responses`))),
    line("  token bucket waits", term(val(paced, "sends the connector buckets delayed"), "=", val(0))),
    line("  breakers",
      ...names.map((n) => term(val(engine.connectors[n].worker.stats.breakerOpens, `${n} opens`), "=", val(0))),
      term(val(isTargetFailure(RATE_LIMIT_STATUS), `status ${RATE_LIMIT_STATUS} counts as a target failure`), "=", val(false))),
    line("quarantined",
      term(val(s.quarantined, "quarantined"), "=", val(s.malformed, "malformed")),
      term(val(s.quarantined, "quarantined"), "=", readme(README.quarantined)),
      term(val(s.quarantineNotes, "notes"), "=", readme(README.quarantineNotes))),
    line("dead-lettered",
      term(val(s.deadLettered, "dead-lettered"), "=", val(WEBHOOK_HARD_FAIL_TASKS, "hard failures")),
      term(val(s.deadLettered, "dead-lettered"), "=", readme(README.deadLettered))),
    line("  dead letters", term(val(s.deadLetterIds, "ids"), "=", val(firstTasks, `the first ${WEBHOOK_HARD_FAIL_TASKS} webhook tasks`))),
    line("DLQ replay",
      term(val(s.replayed, "replayed after clearing the fault"), "=", val(s.deadLettered, "dead-lettered")),
      term(val(s.replayed, "replayed"), "=", readme(README.replayed)),
      term(val(s.dlqAfterReplay, "left in the DLQ"), "=", val(0)),
      term(val(s.webhookDeliveredAfter, "webhook delivered"), "=", val(UNIQUE_PER_CONNECTOR, "unique"))),
    line("  nothing lost", term(val(deliveredAfter, "deliveries across the targets"), "=", val(s.unique, "unique tasks"))),
    line("  no duplicate delivery", ...names.map((n) => term(val(engine.connectors[n].target.inbox.length, `${n} entries`), "=", val(engine.connectors[n].target.seenKeys().size, "keys")))),
    figure("delivery latency", `p50 ${s.p50Ms.toFixed(1)} ms, p95 ${s.p95Ms.toFixed(1)} ms`),
    figure("end-to-end latency", `p50 ${s.e2eP50.toFixed(2)} s, p95 ${s.e2eP95.toFixed(2)} s; queues drained in ${s.drainSeconds.toFixed(1)}s`),
    line("run reproducible", ...reproduced),
  ];
}

/* --------------------------------------------------------------- terraform */

/**
 * Terraform: the shipped resource set, and the diff one new YAML plans. The counts are the
 * real plan's: 7 managed resources per connector (main, dlq, and quarantine queues, the
 * redrive allow policy, IAM policy, role, attachment), one SSM parameter per secret, and the
 * shared table. Shipped: 1 + 3 x 7 + (1 + 2 + 1) = 26; pager-oncall adds 7 + 1 = 8, so 34.
 */
function terraformLines(): CheckLine[] {
  const before = plan(CONNECTOR_YAML);
  const after = plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML });
  const diff = diffPlans(before, after);
  const module = `module.connector["pager-oncall"]`;
  const quarantine = `${module}.module.queue.aws_sqs_queue.quarantine`;
  const inTerraformsWords = `Plan: ${diff.add.length} to add, ${diff.change.length} to change, ${diff.destroy.length} to destroy.`;
  return [
    line("new integration",
      term(val(diff.add.length, "to add"), "=", readme(README.planned)),
      term(val(diff.summary, "summary"), "=", val(inTerraformsWords, "the counts as terraform prints them"))),
    ...diff.add.map((r) => figure("  +", r.address)),
    line("  quarantine queue", term(val(diff.add.filter((r) => r.address === quarantine).length, `planned ${quarantine}`), "=", val(1))),
    line("  only the new module", term(val(diff.add.filter((r) => r.address.startsWith(`${module}.`)).length, `added under ${module}`), "=", val(diff.add.length, "added"))),
    line("  nothing else moves",
      term(val(diff.change.length, "to change"), "=", val(0)),
      term(val(diff.destroy.length, "to destroy"), "=", val(0))),
    line("shipped connectors",
      term(val(before.resources.length, "resources"), "=", readme(README.shippedResources)),
      term(val(before.connectors.length, "connectors"), "=", readme(README.shippedConnectors)),
      term(val(before.resources.filter((r) => r.type === TABLE_RESOURCE.type).length, "shared tables"), "=", val(1))),
    line("  after the fourth",
      term(val(after.resources.length, "resources"), "=", sum(val(before.resources.length, "shipped"), val(diff.add.length, "added"))),
      term(val(after.connectors.length, "connectors"), "=", sum(val(before.connectors.length, "shipped"), val(1, "new")))),
  ];
}

/* ------------------------------------------------------------------- units */

/**
 * conduit/core/idempotency.py's idempotency_key("slack-ops", "T-1", 3) and its revision 4:
 * uv run python -c 'from conduit.core.idempotency import idempotency_key as k; print(k("slack-ops", "T-1", 3))'
 */
const PYTHON_KEYS = {
  revision3: "699d2a7264ef65ef4601d19d31f071f6d24608850759a9b9a43823bc85b0a441",
  revision4: "b3ae1c14e713c9c644b414707797861cb5428d7ef486b10e3207570acb7e17dc",
};

/** Unit assertions on the primitives the scenario depends on. */
async function unitLines(): Promise<CheckLine[]> {
  const lines: CheckLine[] = [];

  const material = keyMaterial("slack-ops", "T-1", 3);
  const key = await idempotencyKey("slack-ops", "T-1", 3);
  const revision4 = await idempotencyKey("slack-ops", "T-1", 4);
  lines.push(line("idempotency key",
    term(val(key, `sha256(${JSON.stringify(material)})`), "=", val(PYTHON_KEYS.revision3, "idempotency.py")),
    term(val(await sha256Hex(material), "that material hashed directly"), "=", val(key, "idempotencyKey()"))));
  lines.push(line("  version is in the key",
    term(val(revision4, "revision 4"), "=", val(PYTHON_KEYS.revision4, "idempotency.py")),
    term(val(revision4, "revision 4"), "!=", val(key, "revision 3"))));

  const remoteId = "1700000000.abc";
  const clock = new Clock();
  const store = new IdempotencyStore(() => clock.now(), 120);
  const first = store.claim(key, "slack-ops", "T-1");
  store.markDelivered(key, remoteId);
  const second = store.claim(key, "slack-ops", "T-1");
  lines.push(line("conditional claim",
    term(val(first.acquired, "first acquires"), "=", val(true)),
    term(val(first.condition, "via"), "=", val("attribute_not_exists")),
    term(val(second.acquired, "second acquires"), "=", val(false)),
    term(val(second.remoteId, "second reads"), "=", val(remoteId, "the delivered remote id"))));

  const lease = store.leaseSeconds;
  const staleKey = await idempotencyKey("slack-ops", "T-2", 1);
  const held = store.claim(staleKey, "slack-ops", "T-2");
  clock.advance(lease / 2);
  const early = store.claim(staleKey, "slack-ops", "T-2");
  clock.advance(lease);
  const late = store.claim(staleKey, "slack-ops", "T-2");
  lines.push(line("  expired lease",
    term(val(held.acquired, "claimed"), "=", val(true)),
    term(val(early.acquired, `claimed again at +${lease / 2}s`), "=", val(false)),
    term(val(late.acquired, `at +${lease * 1.5}s, past the ${lease}s lease`), "=", val(true)),
    term(val(late.condition, "via"), "=", val("expired_lease"))));

  const jira = loadSpec("jira-support", CONNECTOR_YAML["jira-support"]);
  const policy = jira.retry;
  lines.push(line("backoff ceiling",
    term(val(backoffCeiling(3, policy), "attempt 3", "s"), "=", val(Math.min(policy.maxSeconds, policy.baseSeconds * policy.multiplier ** 2), `min(${policy.maxSeconds}, ${policy.baseSeconds} x ${policy.multiplier}^2)`, "s"))));

  const kind = (err: unknown) => (err instanceof TransientError ? "transient" : err instanceof PermanentError ? "permanent" : "no error");
  const throttled = 429;
  const retryAfter = 2;
  const limited = classifyResponse({ status: throttled, headers: { "Retry-After": String(retryAfter) }, body: {} }, policy);
  const plain: [number, string][] = [[400, "permanent"], [503, "transient"]];
  lines.push(line("classification",
    term(val(kind(limited), String(throttled)), "=", val("transient")),
    term(val(limited instanceof TransientError ? limited.retryAfter : null, "with Retry-After", "s"), "=", val(retryAfter, "", "s")),
    ...plain.map(([status, expected]) => term(val(kind(classifyResponse({ status, headers: {}, body: {} }, policy)), String(status)), "=", val(expected)))));

  const rate = 10;
  const burst = 1;
  const cap = 60;
  const bucketClock = new Clock();
  const bucket = new TokenBucket(rate, burst, cap, () => bucketClock.now());
  const firstWait = bucket.acquire();
  const secondWait = bucket.acquire();
  lines.push(line("token bucket",
    term(val(firstWait, `${rate}/s burst ${burst}: the first send waits`, "s"), "=", val(0, "", "s")),
    term(val(secondWait, "the second", "s"), "~=", val(1 / rate, "1/rate", "s"))));
  const asked = 999;
  lines.push(line("  Retry-After capped", term(val(bucket.penalize(asked), `a ${asked}s header pauses sends for`, "s"), "=", val(cap, "the cap", "s"))));

  const { failureThreshold, recoverySeconds } = jira.breaker;
  const breakerClock = new Clock();
  const breaker = new CircuitBreaker(failureThreshold, recoverySeconds, () => breakerClock.now());
  let openedAt = 0;
  for (let failure = 1; failure <= failureThreshold + 1 && openedAt === 0; failure++) if (breaker.recordFailure()) openedAt = failure;
  const whileOpen = breaker.allow();
  breakerClock.advance(recoverySeconds - 1);
  const before = breaker.state;
  breakerClock.advance(1);
  const after = breaker.state;
  const probes = [breaker.allow(), breaker.allow()].filter(Boolean).length;
  lines.push(line("circuit breaker",
    term(val(openedAt, "opens at target failure"), "=", val(failureThreshold, "failure_threshold")),
    term(val(whileOpen, "admits a send while open"), "=", val(false)),
    term(val(before, `state at +${recoverySeconds - 1}s`), "=", val("open")),
    term(val(after, `at +${recoverySeconds}s`), "=", val("half_open")),
    term(val(probes, "probes admitted"), "=", val(1))));
  const closedIt = breaker.recordSuccess();
  lines.push(line("  probe closes it",
    term(val(closedIt, "a successful probe closes an open breaker"), "=", val(true)),
    term(val(breaker.state, "state"), "=", val("closed"))));

  const crm = loadSpec("webhook-crm", CONNECTOR_YAML["webhook-crm"]);
  const maxReceives = crm.queue.maxReceiveCount;
  const qClock = new Clock();
  const main = new Queue("conduit-webhook-crm", () => qClock.now(), 20, "m");
  const dlq = new Queue("conduit-webhook-crm-dlq", () => qClock.now(), 30, "d");
  main.configureRedrive(dlq, maxReceives);
  main.send({ task: makeTask({ id: "T-9", title: "t" }), connector: "webhook-crm", idempotencyKey: key, submittedAt: 0, attempt: 0 });
  let handedOut = 0;
  for (let i = 0; i <= maxReceives; i++) {
    for (const m of main.receive(10)) {
      handedOut += 1;
      main.changeVisibility(m.messageId, 0);
    }
  }
  lines.push(line("redrive",
    term(val(handedOut, "handed to the worker"), "=", val(maxReceives, "maxReceiveCount")),
    term(val(main.redrives[0]?.receiveCount ?? 0, "then moved at receive count"), "=", val(maxReceives, "maxReceiveCount")),
    term(val(main.messages.length, "left in main"), "=", val(0)),
    term(val(dlq.messages.length, "in the DLQ"), "=", val(1))));

  const lab = new Engine("LAB-QUAR");
  const labCrm = lab.connectors["webhook-crm"];
  const unknownStatus = "archived";
  await lab.submit("webhook-crm", [makeTask({ id: "Q-1", title: "valid record" }), makeTask({ id: "Q-2", title: "unknown status", status: unknownStatus })]);
  await lab.drain();
  const note = labCrm.quarantine.messages[0]?.envelope.quarantine;
  lines.push(line("quarantine",
    term(val(note?.stage ?? null, `status ${JSON.stringify(unknownStatus)}: stage`), "=", val("schema")),
    term(val(note?.field ?? null, "field"), "=", val("status")),
    term(val(note?.reason ?? null, "reason"), "=", val("enum")),
    term(val(labCrm.quarantine.messages.length, `in ${labCrm.quarantine.name}`), "=", val(1)),
    term(val(labCrm.dlq.messages.length, "in the DLQ"), "=", val(0)),
    term(val(labCrm.target.inbox.length, "delivered"), "=", val(1))));

  lines.push(line("shipped jira spec",
    term(val(jira.rateLimit.burst, "burst"), "=", readme(README.jira.burst)),
    term(val(jira.breaker.failureThreshold, "breaker failures"), "=", readme(README.jira.failures)),
    term(val(jira.breaker.recoverySeconds, "recovery", "s"), "=", readme(README.jira.recoverySeconds, "s")),
    term(val(jira.rateLimit.maxRetryAfterSeconds, "Retry-After cap", "s"), "=", readme(README.jira.retryAfterCap, "s")),
    term(val(describeRule(jira.mapping.summary), "summary"), "=", val("title (string, required, max_length 255)"))));
  lines.push(line("  constant field", term(val(describeRule(jira.mapping.issuetype), "issuetype"), "=", val("(constant) (default Task)"))));
  lines.push(line("  webhook rules",
    term(val(describeRule(crm.mapping.name), "name"), "=", val("title (required, max_length 200, rejected when longer)")),
    term(val(describeRule(crm.mapping.state), "state"), "=", val("status (one of open|in_progress|blocked|done|closed)")),
    term(val(crm.rateLimit.burst, "burst"), "=", val(10))));

  const mapLab = new Engine("LAB-MAP");
  const mapCrm = mapLab.connectors["webhook-crm"];
  const titleLength = 240;
  await mapLab.submit("webhook-crm", [makeTask({ id: "M-1", title: "x".repeat(titleLength) })]);
  await mapLab.drain();
  const mapNote = mapCrm.quarantine.messages[0]?.envelope.quarantine;
  lines.push(line("  mapping stage",
    term(val(mapNote?.stage ?? null, `a ${titleLength}-character title: stage`), "=", val("mapping")),
    term(val(mapNote?.field ?? null, "field"), "=", val("name")),
    term(val(mapNote?.reason ?? null, "reason"), "=", val("max_length")),
    term(val(mapCrm.quarantine.messages.length, "quarantined"), "=", val(1)),
    term(val(mapLab.store.events.length, "claims attempted"), "=", val(0)),
    term(val(mapCrm.target.inbox.length, "delivered"), "=", val(0))));

  const fourth = loadSpec("pager-oncall", NEW_CONNECTOR_YAML);
  const config = (value: number, unit = "") => val(value, "conduit/config.py", unit);
  lines.push(line("spec defaults",
    term(val(fourth.rateLimit.burst, "pager-oncall burst"), "=", config(1)),
    term(val(fourth.breaker.failureThreshold, "breaker failures"), "=", config(5)),
    term(val(fourth.breaker.recoverySeconds, "recovery", "s"), "=", config(30, "s")),
    term(val(fourth.queue.visibilityTimeoutSeconds, "visibility", "s"), "=", config(60, "s"))));
  let rejected: string | null = null;
  try {
    loadSpec("broken", "type: jira\ntarget: SUP\n");
  } catch (err) {
    rejected = err instanceof Error ? err.message : String(err);
  }
  lines.push(line("  bad YAML rejected", term(val(rejected, "error"), "~", val(/base_url|secrets/))));

  return lines;
}

/** Hand the event loop back so a page stays responsive between assertion groups. */
const yieldToPage = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export async function selfCheck(finished?: FinishedRun): Promise<CheckReport> {
  const scenario = await scenarioLines(finished);
  await yieldToPage();
  const terraform = terraformLines();
  await yieldToPage();
  const lines = [...scenario, ...terraform, ...(await unitLines())];
  const assertions = lines.filter((l) => l.ok !== null);
  const failed = assertions.filter((l) => l.ok === false).length;
  return { lines, ok: failed === 0, passed: assertions.length - failed, failed, assertions: assertions.length };
}

if (proc && Array.isArray(proc.argv) && /selfcheck/.test(proc.argv[1] ?? "")) {
  selfCheck().then(({ lines, ok, passed, failed, assertions }) => {
    console.log("conduit web self-check");
    console.log("=".repeat(78));
    for (const l of lines) console.log(`${l.label.padEnd(24)} ${l.value}${l.ok === false ? "   <-- FAIL" : ""}`);
    console.log("=".repeat(78));
    console.log(`${passed}/${assertions} assertions passed${failed ? `, ${failed} failed` : ""}`);
    console.log(ok ? "all checks passed" : "CHECKS FAILED");
    proc.exitCode = ok ? 0 : 1;
  });
}
