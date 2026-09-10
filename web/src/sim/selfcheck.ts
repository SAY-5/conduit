// Console self-check: the README numbers, reproduced by the browser port, plus
// unit assertions on the pieces those numbers depend on.
// Run with `npm run selfcheck` (Node 20+, Web Crypto via globalThis.crypto).

import { Engine, JIRA_429_ATTEMPTS, JIRA_RATE_LIMITED_TASKS, runScenario, UNIQUE_PER_CONNECTOR, WEBHOOK_HARD_FAIL_TASKS } from "./engine";
import { idempotencyKey, IdempotencyStore, keyMaterial } from "./idempotency";
import { makeTask } from "./models";
import { Clock } from "./prng";
import { Queue } from "./queue";
import { backoffCeiling, classifyResponse, PermanentError, TransientError } from "./retry";
import { CONNECTOR_YAML, loadSpec, NEW_CONNECTOR_YAML } from "./specs";
import { diffPlans, plan } from "./terraform";
import { CircuitBreaker, TokenBucket } from "./throttle";

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

/** The scenario numbers the README prints, checked against the values it prints. */
async function scenarioLines(): Promise<CheckLine[]> {
  const engine = new Engine("D0D3904");
  const s = await runScenario(engine);
  const expectedDeadLetters = Array.from({ length: WEBHOOK_HARD_FAIL_TASKS }, (_, i) => `D0D3904-webhook-crm-${String(i + 1).padStart(4, "0")}`);
  const jira = engine.connectors["jira-support"];
  const totalAfter = Object.values(engine.connectors).reduce((n, rt) => n + rt.target.inbox.length, 0);
  const claimFailures = engine.store.events.filter((e) => e.outcome === "ConditionalCheckFailedException").length;
  const throttled = Object.values(engine.connectors).reduce((n, rt) => n + rt.worker.stats.rateLimitWaits, 0);
  const honored = jira.worker.stats.retryAfterHonored;
  return [
    { label: "tasks submitted", value: `${s.submitted}  (${s.unique} unique + ${s.duplicates} duplicate resubmits)`, ok: s.submitted === 300 },
    { label: "  unique tasks", value: `${s.unique} across ${Object.keys(engine.specs).length} connectors`, ok: s.unique === 240 },
    { label: "  resubmits", value: `${s.duplicates}`, ok: s.duplicates === 60 },
    { label: "deduplicated", value: `${s.deduplicated}  (must equal duplicates: ${s.deduplicated === s.duplicates ? "ok" : "MISMATCH"})`, ok: s.deduplicated === 60 && s.deduplicated === s.duplicates },
    { label: "  conditional puts", value: `${claimFailures} ConditionalCheckFailedException on the idempotency table`, ok: claimFailures >= s.deduplicated },
    ...Object.keys(s.delivered).map((n) => ({
      label: `  ${n}`,
      value: `${s.delivered[n]} delivered, ${s.uniqueKeys[n]} unique keys, ${s.dedupPer[n]} deduplicated`,
      ok: s.uniqueKeys[n] === s.delivered[n] && s.dedupPer[n] === 20,
    })),
    { label: "  jira delivered", value: `${s.delivered["jira-support"]} of ${UNIQUE_PER_CONNECTOR}`, ok: s.delivered["jira-support"] === UNIQUE_PER_CONNECTOR },
    { label: "  slack delivered", value: `${s.delivered["slack-ops"]} of ${UNIQUE_PER_CONNECTOR}`, ok: s.delivered["slack-ops"] === UNIQUE_PER_CONNECTOR },
    { label: "retried", value: `${s.retried}  (jira fake returned 429 ${s.rejected429} times for ${s.rateLimitedTasks} tasks)`, ok: s.retried === 60 },
    { label: "  429 responses", value: `${s.rejected429} = ${JIRA_RATE_LIMITED_TASKS} tasks x ${JIRA_429_ATTEMPTS} attempts`, ok: s.rejected429 === JIRA_RATE_LIMITED_TASKS * JIRA_429_ATTEMPTS },
    { label: "  rate-limited tasks", value: `${s.rateLimitedTasks}`, ok: s.rateLimitedTasks === JIRA_RATE_LIMITED_TASKS },
    { label: "  backoff evidence", value: `attempt 1 -> ${s.byAttempt[1] ?? 0}, attempt 2 -> ${s.byAttempt[2] ?? 0}; delay min/median/max ${s.delayMin.toFixed(3)}s / ${s.delayMedian.toFixed(3)}s / ${s.delayMax.toFixed(3)}s`, ok: s.byAttempt[1] === 30 && s.byAttempt[2] === 30 },
    { label: "  jitter within cap", value: `every delay in [0, ${backoffCeiling(2, jira.spec.retry).toFixed(3)}s]`, ok: s.delayMin >= 0 && s.delayMax <= backoffCeiling(2, jira.spec.retry) },
    { label: "  Retry-After honoured", value: `${honored} 429 responses fed back into the token bucket`, ok: honored === JIRA_RATE_LIMITED_TASKS * JIRA_429_ATTEMPTS },
    { label: "  token bucket waits", value: `${throttled} sends paced by the per-connector bucket`, ok: throttled > 0 },
    { label: "  breakers", value: `0 opens (429 is throttling, not a target failure)`, ok: Object.values(engine.connectors).every((rt) => rt.worker.stats.breakerOpens === 0) },
    { label: "dead-lettered", value: `${s.deadLettered}  (must equal hard failures ${WEBHOOK_HARD_FAIL_TASKS}: ${s.deadLettered === WEBHOOK_HARD_FAIL_TASKS ? "ok" : "MISMATCH"})`, ok: s.deadLettered === 10 },
    { label: "  dead letters", value: s.deadLetterIds.map((t) => t.slice(-4)).join(", "), ok: JSON.stringify(s.deadLetterIds) === JSON.stringify(expectedDeadLetters) },
    { label: "DLQ replay", value: `${s.replayed} replayed after clearing the fault; DLQ now ${s.dlqAfterReplay}; webhook delivered ${s.webhookDeliveredAfter}/80`, ok: s.replayed === 10 && s.dlqAfterReplay === 0 && s.webhookDeliveredAfter === 80 },
    { label: "  nothing lost", value: `${totalAfter} deliveries across the three targets`, ok: totalAfter === 3 * UNIQUE_PER_CONNECTOR },
    { label: "  no duplicate delivery", value: "each target inbox holds one entry per idempotency key", ok: Object.values(engine.connectors).every((rt) => rt.target.seenKeys().size === rt.target.inbox.length) },
    { label: "delivery latency", value: `p50 ${s.p50Ms.toFixed(1)} ms, p95 ${s.p95Ms.toFixed(1)} ms`, ok: null },
    { label: "end-to-end latency", value: `p50 ${s.e2eP50.toFixed(2)} s, p95 ${s.e2eP95.toFixed(2)} s; queues drained in ${s.drainSeconds.toFixed(1)}s`, ok: null },
    { label: "run reproducible", value: `seed ${s.runId}, virtual clock, no Math.random or Date.now in src/sim`, ok: s.ok },
  ];
}

/** Terraform: the shipped resource set, and the diff one new YAML plans. */
function terraformLines(): CheckLine[] {
  const before = plan(CONNECTOR_YAML);
  const after = plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML });
  const diff = diffPlans(before, after);
  const sameName = diff.add.every((r) => r.address.includes(`["pager-oncall"]`));
  return [
    { label: "new integration", value: diff.summary, ok: diff.add.length === 7 && diff.change.length === 0 && diff.destroy.length === 0 },
    ...diff.add.map((r) => ({ label: "  +", value: r.address, ok: null })),
    { label: "  only the new module", value: "every added address is under module.connector[\"pager-oncall\"]", ok: sameName },
    { label: "  nothing else moves", value: `${diff.change.length} to change, ${diff.destroy.length} to destroy`, ok: diff.change.length === 0 && diff.destroy.length === 0 },
    { label: "shipped connectors", value: `${before.resources.length} resources for ${before.connectors.length} connectors plus the shared table`, ok: before.resources.length === 23 && before.connectors.length === 3 },
    { label: "  after the fourth", value: `${after.resources.length} resources for ${after.connectors.length} connectors`, ok: after.resources.length === 30 },
  ];
}

/** Unit assertions on the primitives the scenario depends on. */
async function unitLines(): Promise<CheckLine[]> {
  const lines: CheckLine[] = [];

  const material = keyMaterial("slack-ops", "T-1", 3);
  const key = await idempotencyKey("slack-ops", "T-1", 3);
  const other = await idempotencyKey("slack-ops", "T-1", 4);
  lines.push({ label: "idempotency key", value: `sha256("${material}") = ${key.slice(0, 16)}...`, ok: /^[0-9a-f]{64}$/.test(key) });
  lines.push({ label: "  version is in the key", value: `revision 4 hashes to ${other.slice(0, 16)}...`, ok: key !== other });

  const clock = new Clock();
  const store = new IdempotencyStore(() => clock.now(), 120);
  const first = store.claim(key, "slack-ops", "T-1");
  store.markDelivered(key, "1700000000.abc");
  const second = store.claim(key, "slack-ops", "T-1");
  lines.push({ label: "conditional claim", value: `first acquires (${first.condition}), second fails with the delivered remote id`, ok: first.acquired && !second.acquired && second.remoteId === "1700000000.abc" });
  clock.advance(200);
  const held = store.claim(await idempotencyKey("slack-ops", "T-2", 1), "slack-ops", "T-2");
  clock.advance(200);
  const stolen = store.claim(await idempotencyKey("slack-ops", "T-2", 1), "slack-ops", "T-2");
  lines.push({ label: "  expired lease", value: `a stale in_progress claim is retaken via ${stolen.condition}`, ok: held.acquired && stolen.acquired && stolen.condition === "expired_lease" });

  const jira = loadSpec("jira-support", CONNECTOR_YAML["jira-support"]);
  lines.push({ label: "backoff ceiling", value: `attempt 3 -> min(${jira.retry.maxSeconds}, ${jira.retry.baseSeconds} x ${jira.retry.multiplier}^2) = ${backoffCeiling(3, jira.retry)}s`, ok: backoffCeiling(3, jira.retry) === Math.min(jira.retry.maxSeconds, jira.retry.baseSeconds * jira.retry.multiplier ** 2) });
  const rl = classifyResponse({ status: 429, headers: { "Retry-After": "2" }, body: {} }, jira.retry);
  const hard = classifyResponse({ status: 400, headers: {}, body: {} }, jira.retry);
  const down = classifyResponse({ status: 503, headers: {}, body: {} }, jira.retry);
  lines.push({ label: "classification", value: "429 transient (Retry-After 2s), 400 permanent, 503 transient", ok: rl instanceof TransientError && (rl as TransientError).retryAfter === 2 && hard instanceof PermanentError && down instanceof TransientError });

  const bucketClock = new Clock();
  const bucket = new TokenBucket(10, 1, 60, () => bucketClock.now());
  const w1 = bucket.acquire();
  const w2 = bucket.acquire();
  lines.push({ label: "token bucket", value: `10 req/s, burst 1: first send waits ${w1.toFixed(3)}s, second ${w2.toFixed(3)}s`, ok: w1 === 0 && Math.abs(w2 - 0.1) < 1e-9 });
  const pause = bucket.penalize(999);
  lines.push({ label: "  Retry-After capped", value: `a 999s header pauses the connector for ${pause}s, not 999s`, ok: pause === 60 });

  const breakerClock = new Clock();
  const breaker = new CircuitBreaker(5, 30, () => breakerClock.now());
  let opened = false;
  for (let i = 0; i < 5; i++) opened = breaker.recordFailure();
  const blocked = !breaker.allow();
  breakerClock.advance(30);
  const probe = breaker.allow();
  const secondProbe = breaker.allow();
  lines.push({ label: "circuit breaker", value: `opens after 5 target failures, blocks while open, half-opens after 30s`, ok: opened && blocked && breaker.state === "half_open" && probe && !secondProbe });
  const closed = breaker.recordSuccess();
  lines.push({ label: "  probe closes it", value: `a successful probe returns the breaker to ${breaker.state}`, ok: closed && breaker.state === "closed" });

  const qClock = new Clock();
  const main = new Queue("conduit-webhook-crm", () => qClock.now(), 20, "m");
  const dlq = new Queue("conduit-webhook-crm-dlq", () => qClock.now(), 30, "d");
  main.configureRedrive(dlq, 2);
  main.send({ task: makeTask({ id: "T-9", title: "t" }), connector: "webhook-crm", idempotencyKey: key, submittedAt: 0, attempt: 0 });
  for (let i = 0; i < 3; i++) {
    for (const m of main.receive(10)) main.changeVisibility(m.messageId, 0);
  }
  lines.push({ label: "redrive", value: `maxReceiveCount=2 moves the message after ${main.redrives[0]?.receiveCount ?? 0} receives; main ${main.messages.length}, dlq ${dlq.messages.length}`, ok: main.messages.length === 0 && dlq.messages.length === 1 });

  const fourth = loadSpec("pager-oncall", NEW_CONNECTOR_YAML);
  lines.push({ label: "spec defaults", value: `pager-oncall: burst ${fourth.rateLimit.burst}, breaker ${fourth.breaker.failureThreshold}/${fourth.breaker.recoverySeconds}s, visibility ${fourth.queue.visibilityTimeoutSeconds}s`, ok: fourth.rateLimit.burst === 1 && fourth.breaker.failureThreshold === 5 && fourth.queue.visibilityTimeoutSeconds === 60 });
  let rejected = "";
  try {
    loadSpec("broken", "type: jira\ntarget: SUP\n");
  } catch (err) {
    rejected = err instanceof Error ? err.message : String(err);
  }
  lines.push({ label: "  bad YAML rejected", value: rejected || "no error raised", ok: /base_url|secrets/.test(rejected) });

  return lines;
}

export async function selfCheck(): Promise<CheckReport> {
  const lines = [...(await scenarioLines()), ...terraformLines(), ...(await unitLines())];
  const assertions = lines.filter((l) => l.ok !== null);
  const failed = assertions.filter((l) => l.ok === false).length;
  return { lines, ok: failed === 0, passed: assertions.length - failed, failed, assertions: assertions.length };
}

interface NodeProcess {
  argv: string[];
  exitCode?: number;
}
const proc = (globalThis as { process?: NodeProcess }).process;
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
