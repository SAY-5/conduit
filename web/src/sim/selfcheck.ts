// Console self-check: the README numbers, reproduced by the browser port.
// Run with `npm run selfcheck` (Node 20+, Web Crypto via globalThis.crypto).

import { Engine, runScenario, WEBHOOK_HARD_FAIL_TASKS } from "./engine";
import { CONNECTOR_YAML, NEW_CONNECTOR_YAML } from "./specs";
import { diffPlans, plan } from "./terraform";

export interface CheckLine {
  label: string;
  value: string;
  ok: boolean | null;
}

export async function selfCheck(): Promise<{ lines: CheckLine[]; ok: boolean }> {
  const engine = new Engine("D0D3904");
  const s = await runScenario(engine);
  const before = plan(CONNECTOR_YAML);
  const after = plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML });
  const diff = diffPlans(before, after);
  const lines: CheckLine[] = [
    { label: "tasks submitted", value: `${s.submitted}  (${s.unique} unique + ${s.duplicates} duplicate resubmits)`, ok: s.submitted === 300 },
    { label: "deduplicated", value: `${s.deduplicated}  (must equal duplicates: ${s.deduplicated === s.duplicates ? "ok" : "MISMATCH"})`, ok: s.deduplicated === s.duplicates },
    ...Object.keys(s.delivered).map((n) => ({ label: `  ${n}`, value: `${s.delivered[n]} delivered, ${s.uniqueKeys[n]} unique keys, ${s.dedupPer[n]} deduplicated`, ok: null })),
    { label: "retried", value: `${s.retried}  (jira fake returned 429 ${s.rejected429} times for ${s.rateLimitedTasks} tasks)`, ok: s.retried === 60 },
    { label: "  backoff evidence", value: `attempt 1 -> ${s.byAttempt[1] ?? 0}, attempt 2 -> ${s.byAttempt[2] ?? 0}; delay min/median/max ${s.delayMin.toFixed(3)}s / ${s.delayMedian.toFixed(3)}s / ${s.delayMax.toFixed(3)}s`, ok: s.delayMax <= 0.5 },
    { label: "dead-lettered", value: `${s.deadLettered}  (must equal hard failures ${WEBHOOK_HARD_FAIL_TASKS}: ${s.deadLettered === WEBHOOK_HARD_FAIL_TASKS ? "ok" : "MISMATCH"})`, ok: s.deadLettered === WEBHOOK_HARD_FAIL_TASKS },
    { label: "  dead letters", value: s.deadLetterIds.map((t) => t.slice(-4)).join(", "), ok: null },
    { label: "DLQ replay", value: `${s.replayed} replayed after clearing the fault; DLQ now ${s.dlqAfterReplay}; webhook delivered ${s.webhookDeliveredAfter}/80`, ok: s.dlqAfterReplay === 0 && s.webhookDeliveredAfter === 80 },
    { label: "delivery latency", value: `p50 ${s.p50Ms.toFixed(1)} ms, p95 ${s.p95Ms.toFixed(1)} ms`, ok: null },
    { label: "end-to-end latency", value: `p50 ${s.e2eP50.toFixed(2)} s, p95 ${s.e2eP95.toFixed(2)} s; queues drained in ${s.drainSeconds.toFixed(1)}s`, ok: null },
    { label: "new integration", value: diff.summary, ok: diff.add.length === 7 && diff.change.length === 0 && diff.destroy.length === 0 },
    ...diff.add.map((r) => ({ label: "  +", value: r.address, ok: null })),
    { label: "shipped connectors", value: `${before.resources.length} resources`, ok: before.resources.length === 23 },
  ];
  const ok = s.ok && lines.every((l) => l.ok !== false);
  return { lines, ok };
}

interface NodeProcess {
  argv: string[];
  exitCode?: number;
}
const proc = (globalThis as { process?: NodeProcess }).process;
if (proc && Array.isArray(proc.argv) && /selfcheck/.test(proc.argv[1] ?? "")) {
  selfCheck().then(({ lines, ok }) => {
    console.log("conduit web self-check");
    console.log("=".repeat(72));
    for (const l of lines) console.log(`${l.label.padEnd(22)} ${l.value}${l.ok === false ? "   <-- FAIL" : ""}`);
    console.log("=".repeat(72));
    console.log(ok ? "all checks passed" : "CHECKS FAILED");
    proc.exitCode = ok ? 0 : 1;
  });
}
