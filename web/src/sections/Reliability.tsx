import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useMemo, useState } from "react";
import { Chip, Reveal, SectionHead } from "../components/common";
import { LogStream } from "../components/LogStream";
import { Engine } from "../sim/engine";
import type { StoreEvent, StoreItem } from "../sim/idempotency";
import { makeTask } from "../sim/models";
import { backoffCeiling, type RetryAttempt } from "../sim/retry";
import type { HandleTrace, LogEvent } from "../sim/worker";
import "./reliability.css";

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function useLab(seed: string) {
  const engine = useMemo(() => new Engine(seed), [seed]);
  const [, bump] = useState(0);
  const refresh = useCallback(() => bump((n) => n + 1), []);
  return { engine, refresh };
}

/* Panel A: conditional claim */
function ClaimLab() {
  const reduced = useReducedMotion();
  const { engine, refresh } = useLab("LAB-CLAIM");
  const [busy, setBusy] = useState(false);
  const [version, setVersion] = useState(0);
  const [last, setLast] = useState<HandleTrace | null>(null);
  const rt = engine.connectors["jira-support"];
  const taskId = "TASK-7";

  const deliver = useCallback(
    async (v: number) => {
      setBusy(true);
      const task = makeTask({ id: taskId, version: v, title: v === 1 ? "Rotate the PSP webhook secret" : "Rotate the PSP webhook secret (blocked on vendor)", body: `revision ${v}`, priority: "high", labels: ["billing"] });
      await engine.submit("jira-support", [task]);
      refresh();
      await wait(reduced ? 0 : 350);
      const traces = await rt.worker.poll(1);
      engine.clock.advance(0.1);
      setLast(traces[0] ?? null);
      refresh();
      setBusy(false);
    },
    [engine, rt, refresh, reduced],
  );

  const items: StoreItem[] = Array.from(engine.store.items.values());
  const events: StoreEvent[] = engine.store.events.slice(-6);
  const latest = engine.store.latest("jira-support", taskId);

  return (
    <div className="lab glass">
      <div className="lab-head">
        <span className="eyebrow">conditional claim</span>
        <h3>Resubmit the same revision; the claim short-circuits.</h3>
        <p>The worker claims the key with one conditional PutItem before delivering. A second message carrying the same key fails the condition, sees <code>state=delivered</code>, and is acknowledged as deduplicated without touching Jira. A new revision derives a new key and updates the recorded issue.</p>
      </div>
      <div className="lab-actions">
        <button className="btn btn-primary" disabled={busy || version >= 1} onClick={() => { setVersion(1); void deliver(1); }}>Submit {taskId} v1</button>
        <button className="btn" disabled={busy || version < 1} onClick={() => void deliver(1)}>Resubmit v1 (duplicate)</button>
        <button className="btn" disabled={busy || version < 1} onClick={() => { setVersion(2); void deliver(2); }}>Submit v2 (new revision)</button>
        <button className="btn btn-sm" disabled={busy} onClick={() => { engine.reset(); setVersion(0); setLast(null); refresh(); }}>Reset</button>
      </div>
      <AnimatePresence mode="wait">
        {last ? (
          <motion.div key={last.message.messageId + last.reason} className={`verdict verdict-${last.reason}`} initial={reduced ? false : { opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            {last.reason === "deduplicated" ? (
              <>
                <strong>ConditionalCheckFailedException</strong> · item already <code>delivered</code> with remote_id <code>{last.claim.remoteId}</code> · message deleted, counted as <code>deduplicated</code>, Jira never called
              </>
            ) : (
              <>
                <strong>claim acquired</strong> via <code>{last.claim.condition}</code> · {last.message.envelope.task.version > 1 ? <>revision {last.message.envelope.task.version} updated <code>{last.result?.remoteId}</code> through the <code>latest|jira-support|{taskId}</code> pointer</> : <>issue <code>{last.result?.remoteId}</code> created</>} · marked <code>delivered</code>
              </>
            )}
          </motion.div>
        ) : null}
      </AnimatePresence>
      <div className="lab-grid">
        <div>
          <span className="code-label">DynamoDB conduit-idempotency (pk, state, remote_id, lease)</span>
          <table className="table">
            <thead><tr><th>pk</th><th>state</th><th>remote_id</th><th>lease_until</th></tr></thead>
            <tbody>
              {items.length === 0 ? <tr><td colSpan={4} className="muted">empty</td></tr> : null}
              {items.map((it) => (
                <tr key={it.pk}>
                  <td className="mono">{it.pk.slice(0, 14)}…</td>
                  <td><Chip tone={it.state === "delivered" ? "chip-teal" : "chip-warn"}>{it.state}</Chip></td>
                  <td className="mono">{it.remoteId ?? "-"}</td>
                  <td className="mono">t+{it.leaseUntil.toFixed(0)}s</td>
                </tr>
              ))}
              {latest ? <tr><td className="mono">latest|jira-support|{taskId}</td><td><Chip>pointer</Chip></td><td className="mono">{latest}</td><td className="mono">-</td></tr> : null}
            </tbody>
          </table>
        </div>
        <div>
          <span className="code-label">store calls</span>
          <ul className="store-events mono">
            {events.length === 0 ? <li className="muted">no calls yet</li> : null}
            {events.map((e, i) => (
              <li key={i} className={e.outcome === "ok" ? "ev-ok" : "ev-fail"}>
                <b>{e.op}</b> <span className="muted">{e.condition}</span> → {e.outcome}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

/* Panel B: 429 burst with backoff */
function BackoffLab() {
  const reduced = useReducedMotion();
  const { engine, refresh } = useLab("LAB-429");
  const [count, setCount] = useState(2);
  const [busy, setBusy] = useState(false);
  const [trace, setTrace] = useState<HandleTrace | null>(null);
  const [shownAttempts, setShownAttempts] = useState(0);
  const rt = engine.connectors["jira-support"];
  const policy = rt.spec.retry;
  const taskId = "TASK-429";

  const run = useCallback(async () => {
    setBusy(true);
    engine.reset();
    rt.target.setFaults({ rateLimitTasks: new Set([taskId]), rateLimitCount: count });
    await engine.submit("jira-support", [makeTask({ id: taskId, title: "Nightly reconciliation report", priority: "normal" })]);
    const traces = await rt.worker.poll(1);
    const t = traces[0];
    setTrace(t);
    setShownAttempts(0);
    refresh();
    const n = t.retry?.trace.length ?? 0;
    for (let i = 1; i <= n; i++) {
      await wait(reduced ? 0 : 420);
      setShownAttempts(i);
    }
    setBusy(false);
  }, [engine, rt, count, refresh, reduced]);

  const attempts: RetryAttempt[] = trace?.retry?.trace ?? [];
  const maxCeiling = Math.max(...Array.from({ length: policy.maxAttempts - 1 }, (_, i) => backoffCeiling(i + 1, policy)));

  return (
    <div className="lab glass">
      <div className="lab-head">
        <span className="eyebrow">transient failures</span>
        <h3>Inject a 429 burst and watch full-jitter backoff.</h3>
        <p>jira-support retries up to <code>max_attempts={policy.maxAttempts}</code> inside one SQS receive: delay = uniform(0, min({policy.maxSeconds}s, {policy.baseSeconds}s × 2^(attempt-1))). Each sleep first extends the message's visibility so it cannot be redelivered mid-retry. With {policy.maxAttempts} attempts, {policy.maxAttempts - 1} consecutive 429s still deliver; {policy.maxAttempts} exhaust the budget and the message goes back to the queue for redrive.</p>
      </div>
      <div className="lab-actions">
        <div className="seg" role="group" aria-label="How many 429 responses">
          {[1, 2, 3, 4].map((n) => (
            <button key={n} aria-pressed={n === count} onClick={() => setCount(n)} disabled={busy}>{n} × 429</button>
          ))}
        </div>
        <button className="btn btn-warn" onClick={() => void run()} disabled={busy}>Inject and deliver</button>
      </div>
      <div className="attempts" aria-live="polite">
        {attempts.length === 0 ? <p className="muted">Pick a burst size and deliver. The fake sets Retry-After: 0, so the jitter floor stays at zero.</p> : null}
        {attempts.slice(0, shownAttempts).map((a) => (
          <motion.div key={a.attempt} className={`attempt attempt-${a.outcome}`} initial={reduced ? false : { opacity: 0, x: -10 }} animate={{ opacity: 1, x: 0 }}>
            <span className="attempt-n mono">attempt {a.attempt}</span>
            <span className="attempt-status mono">{a.outcome === "ok" ? `${trace?.result?.remoteId ? "201 created " + trace.result.remoteId : "ok"}` : `HTTP ${a.status} ${a.outcome === "transient" ? "TransientError" : a.outcome === "exhausted" ? "TransientError, budget exhausted" : "PermanentError"}`}</span>
            {a.delay !== null && a.ceiling !== null ? (
              <span className="attempt-bar" title={`ceiling ${a.ceiling.toFixed(3)}s, slept ${a.delay.toFixed(3)}s`}>
                <span className="attempt-ceiling" style={{ width: `${(a.ceiling / maxCeiling) * 100}%` }} />
                <motion.span className="attempt-delay" initial={reduced ? false : { width: 0 }} animate={{ width: `${(a.delay / maxCeiling) * 100}%` }} transition={{ duration: 0.5 }} />
                <span className="attempt-label mono">slept {a.delay.toFixed(3)}s of ≤{a.ceiling.toFixed(2)}s</span>
              </span>
            ) : (
              <span className="attempt-bar attempt-bar-empty mono">{a.outcome === "ok" ? `delivered after ${a.attempt} attempt${a.attempt > 1 ? "s" : ""}, ${((trace?.elapsed ?? 0) * 1000).toFixed(0)} ms attempt-to-ack` : "claim released, visibility set to 0, SQS redelivers"}</span>
            )}
          </motion.div>
        ))}
      </div>
      <LogStream events={engine.log.filter((e) => e.kind !== "submit").slice(-8) as LogEvent[]} limit={8} label="worker log" />
    </div>
  );
}

/* Panel C: hard 400 bouncing into the DLQ */
function HardFailLab() {
  const reduced = useReducedMotion();
  const { engine, refresh } = useLab("LAB-400");
  const [busy, setBusy] = useState(false);
  const [steps, setSteps] = useState<{ receive: number; label: string; kind: string }[]>([]);
  const rt = engine.connectors["webhook-crm"];
  const max = rt.spec.queue.maxReceiveCount;
  const taskId = "TASK-400";

  const run = useCallback(async () => {
    setBusy(true);
    engine.reset();
    setSteps([]);
    rt.target.setFaults({ hardFailTasks: new Set([taskId]) });
    await engine.submit("webhook-crm", [makeTask({ id: taskId, title: "Contact with malformed region code", priority: "low" })]);
    refresh();
    for (let i = 0; i < max + 1; i++) {
      await wait(reduced ? 0 : 650);
      const traces = await rt.worker.poll(1);
      engine.clock.advance(0.1);
      const t = traces[0];
      if (t) {
        setSteps((s) => [...s, { receive: t.message.receiveCount, kind: t.willDeadLetter ? "dlq" : "fail", label: `receive #${t.message.receiveCount}: claim → POST → HTTP 400 → PermanentError → release claim → visibility 0${t.willDeadLetter ? " → receive count reached maxReceiveCount" : ""}` }]);
      } else {
        setSteps((s) => [...s, { receive: max + 1, kind: "moved", label: `receive #${max + 1} would exceed maxReceiveCount=${max}: SQS moves the message to ${rt.dlq.name}` }]);
      }
      refresh();
    }
    setBusy(false);
  }, [engine, rt, max, refresh, reduced]);

  const inDlq = rt.dlq.messages.length;
  const inQueue = rt.queue.messages.length;
  const receiveCount = rt.queue.messages[0]?.receiveCount ?? (inDlq ? max : 0);

  return (
    <div className="lab glass">
      <div className="lab-head">
        <span className="eyebrow">permanent failures</span>
        <h3>Inject a hard 400 and watch the message bounce into the DLQ.</h3>
        <p>A 4xx outside the retry list is a <code>PermanentError</code>: no in-process retry. The worker releases the claim and sets visibility to 0 so SQS redelivers immediately. webhook-crm sets <code>max_receive_count: {max}</code>, so after {max} failed receives the queue itself moves the message to <code>{rt.dlq.name}</code>.</p>
      </div>
      <div className="lab-actions">
        <button className="btn btn-danger" onClick={() => void run()} disabled={busy}>Inject 400 and deliver</button>
      </div>
      <div className="bounce-board">
        <div className={`bounce-queue ${inQueue ? "bounce-on" : ""}`}>
          <span className="code-label">{rt.queue.name}</span>
          <div className="pips" aria-label={`receive count ${receiveCount} of ${max}`}>
            {Array.from({ length: max }, (_, i) => <span key={i} className={`pip ${i < receiveCount ? "pip-on" : ""}`} />)}
          </div>
          <span className="mono muted">ApproximateReceiveCount {receiveCount} / maxReceiveCount {max}</span>
        </div>
        <span className="bounce-arrow mono" aria-hidden>{inDlq ? "redrive →" : "→"}</span>
        <div className={`bounce-dlq ${inDlq ? "bounce-hot" : ""}`}>
          <span className="code-label">{rt.dlq.name}</span>
          <strong className="mono">{inDlq}</strong>
          <span className="mono muted">{inDlq ? `${taskId} v1, claim released, ready for replay` : "empty"}</span>
        </div>
      </div>
      <ol className="steps">
        {steps.map((s, i) => (
          <motion.li key={i} className={`step step-${s.kind}`} initial={reduced ? false : { opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }}>
            <span className="mono">{s.label}</span>
          </motion.li>
        ))}
      </ol>
    </div>
  );
}

export function Reliability() {
  return (
    <section className="section" id="reliability" aria-labelledby="reliability-title">
      <div className="wrap">
        <SectionHead eyebrow="02 / idempotency + retries" id="reliability-title" title="Nothing twice, nothing lost: claim, classify, back off." lede="Three small labs against the same worker code the hero runs. Each one uses its own queue pair, claim store, and fake target with runtime fault injection." />
        <div className="labs">
          <Reveal><ClaimLab /></Reveal>
          <Reveal delay={0.05}><BackoffLab /></Reveal>
          <Reveal delay={0.1}><HardFailLab /></Reveal>
        </div>
      </div>
    </section>
  );
}
