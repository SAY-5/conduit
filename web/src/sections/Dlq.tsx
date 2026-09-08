import { motion, useReducedMotion } from "framer-motion";
import { useCallback, useMemo, useState } from "react";
import { Chip, Reveal, SectionHead } from "../components/common";
import { LogStream } from "../components/LogStream";
import { Engine } from "../sim/engine";
import { makeTask } from "../sim/models";
import "./dlq.css";

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const COUNT = 10;

export function Dlq() {
  const reduced = useReducedMotion();
  const engine = useMemo(() => new Engine("LAB-DLQ"), []);
  const [, bump] = useState(0);
  const refresh = useCallback(() => bump((n) => n + 1), []);
  const [busy, setBusy] = useState(false);
  const [stage, setStage] = useState<"empty" | "loaded" | "replaying" | "drained">("empty");
  const rt = engine.connectors["webhook-crm"];
  const faultOn = rt.target.faults.hardFailTasks.size > 0;

  const drain = useCallback(async () => {
    let idle = 0;
    while (idle < 3) {
      const traces = await rt.worker.poll(3);
      engine.clock.advance(0.1);
      refresh();
      if (!traces.length && rt.queue.messages.length === 0) idle += 1;
      else idle = 0;
      await wait(reduced ? 0 : 90);
    }
  }, [engine, rt, refresh, reduced]);

  const load = useCallback(async () => {
    setBusy(true);
    engine.reset();
    const tasks = Array.from({ length: COUNT }, (_, i) => makeTask({ id: `${engine.runId}-webhook-crm-${String(i + 1).padStart(4, "0")}`, title: `Synthetic task ${i + 1} for webhook-crm`, body: `Body of task ${i + 1}`, priority: "normal", labels: ["webhook", "demo"] }));
    rt.target.setFaults({ hardFailTasks: new Set(tasks.map((t) => t.id)) });
    await engine.submit("webhook-crm", tasks);
    refresh();
    await drain();
    setStage("loaded");
    setBusy(false);
  }, [engine, rt, refresh, drain]);

  const clearFault = useCallback(() => {
    rt.target.clearFaults();
    engine.emit({ connector: "webhook-crm", kind: "info", event: "faults.cleared", taskId: "", key: "", detail: "DELETE /_faults on the webhook fake" });
    refresh();
  }, [rt, engine, refresh]);

  const replay = useCallback(async () => {
    setBusy(true);
    setStage("replaying");
    engine.replay("webhook-crm");
    refresh();
    await wait(reduced ? 0 : 500);
    await drain();
    setStage(rt.dlq.messages.length ? "loaded" : "drained");
    setBusy(false);
  }, [engine, rt, refresh, drain, reduced]);

  const dead = rt.dlq.messages;
  const inbox = rt.target.inbox;
  const replayedCount = inbox.filter((e) => e.replayed).length;

  return (
    <section className="section" id="dlq" aria-labelledby="dlq-title">
      <div className="wrap">
        <SectionHead eyebrow="03 / dead-letter queue + replay" id="dlq-title" title="Dead letters wait. Fix the integration, replay, drain to zero." lede="The worker only deletes a message after a successful delivery or a confirmed duplicate, so exhaustion is the only path into the DLQ. Replay moves messages back with attempt + 1 and a fresh receive count; the claim was released on failure, so a replay after the fix delivers normally." />
        <Reveal className="dlq-grid" delay={0.1}>
          <div className="glass dlq-panel">
            <div className="dlq-cli mono">
              <span className="muted">$</span> conduit dlq list -c webhook-crm
            </div>
            <div className="dlq-meters">
              <div className={`meter ${dead.length ? "meter-hot" : ""}`}>
                <span className="stat-label">conduit-webhook-crm-dlq</span>
                <strong className="mono">{dead.length}</strong>
                <div className="meter-bar" aria-hidden>
                  <motion.span animate={{ width: `${(dead.length / COUNT) * 100}%` }} transition={{ duration: reduced ? 0 : 0.35 }} />
                </div>
              </div>
              <div className="meter meter-inbox">
                <span className="stat-label">webhook fake inbox</span>
                <strong className="mono">{inbox.length}</strong>
                <div className="meter-bar" aria-hidden>
                  <motion.span animate={{ width: `${(inbox.length / COUNT) * 100}%` }} transition={{ duration: reduced ? 0 : 0.35 }} />
                </div>
              </div>
              <div className="meter">
                <span className="stat-label">fault: 400 for {COUNT} tasks</span>
                <strong className={`mono ${faultOn ? "hot" : "ok"}`}>{faultOn ? "ON" : "OFF"}</strong>
                <span className="muted mono small">{faultOn ? "POST /_faults hard_fail_tasks" : "DELETE /_faults"}</span>
              </div>
            </div>
            <div className="lab-actions">
              <button className="btn btn-primary" onClick={() => void load()} disabled={busy}>{stage === "empty" ? `Send ${COUNT} tasks into the fault` : "Reset and reload"}</button>
              <button className="btn btn-warn" onClick={clearFault} disabled={busy || !faultOn || stage === "empty"}>Clear the fault</button>
              <button className="btn" onClick={() => void replay()} disabled={busy || dead.length === 0}>conduit dlq replay</button>
            </div>
            <p className="dlq-hint">
              {stage === "empty" ? `Queue ${COUNT} tasks the webhook receiver rejects with 400. Each one is received twice (maxReceiveCount=2) and then moved to the DLQ by SQS.` : null}
              {stage === "loaded" && faultOn ? "Replaying now would bounce every message straight back into the DLQ with replays + 1. Clear the fault first, or try it and see." : null}
              {stage === "loaded" && !faultOn ? "Fault cleared. Replay moves the dead letters back onto conduit-webhook-crm; the worker claims each key again and delivers." : null}
              {stage === "replaying" ? "Replaying: messages are back on the work queue with attempt + 1." : null}
              {stage === "drained" ? `DLQ now 0; webhook delivered ${inbox.length}/${COUNT}, ${replayedCount} of them on replay. The fake dedupes on Idempotency-Key, so a second replay could never double-post.` : null}
            </p>
            <table className="table dlq-table">
              <thead><tr><th>task</th><th>ver</th><th>key</th><th>receives</th><th>replays</th><th>dead-lettered at</th></tr></thead>
              <tbody>
                {dead.length === 0 ? <tr><td colSpan={6} className="muted">{stage === "drained" ? "no dead letters" : "empty"}</td></tr> : null}
                {dead.map((m) => (
                  <tr key={m.messageId}>
                    <td className="mono">{m.envelope.task.id.replace(`${engine.runId}-`, "")}</td>
                    <td className="mono">{m.envelope.task.version}</td>
                    <td className="mono">{m.envelope.idempotencyKey.slice(0, 12)}…</td>
                    <td className="mono">{m.receiveCount}</td>
                    <td className="mono">{m.replays}</td>
                    <td className="mono">t+{(m.deadLetteredAt ?? 0).toFixed(1)}s</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="dlq-side">
            <div className="glass dlq-panel">
              <span className="code-label">webhook fake /_inbox (dedupes on Idempotency-Key)</span>
              <ul className="inbox-list mono">
                {inbox.length === 0 ? <li className="muted">nothing received</li> : null}
                {inbox.slice(-COUNT).map((e) => (
                  <motion.li key={e.seq} initial={reduced ? false : { opacity: 0, x: 8 }} animate={{ opacity: 1, x: 0 }}>
                    <span>{e.taskId.replace(`${engine.runId}-`, "")}</span>
                    <span className="muted">{e.remoteId}</span>
                    {e.replayed ? <Chip tone="chip-teal">replayed</Chip> : <Chip>first pass</Chip>}
                  </motion.li>
                ))}
              </ul>
            </div>
            <LogStream events={engine.log.filter((e) => e.kind !== "submit" && e.kind !== "claim")} limit={12} label="worker log" />
          </div>
        </Reveal>
      </div>
    </section>
  );
}
