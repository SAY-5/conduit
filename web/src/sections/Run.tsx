import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Chip, Counter, Reveal, SectionHead, TYPE_TONE } from "../components/common";
import { LogStream } from "../components/LogStream";
import { capturePhaseOne, Engine, formatSummary, setupScenario, summarize, type ScenarioSetup, type Summary } from "../sim/engine";
import { selfCheck, type CheckReport } from "../sim/selfcheck";
import { CONNECTOR_YAML, NEW_CONNECTOR_YAML } from "../sim/specs";
import { diffPlans, plan } from "../sim/terraform";
import type { LogEvent } from "../sim/worker";
import "./run.css";

type Stage = "idle" | "submitting" | "draining" | "clearing" | "replaying" | "done";

const STAGE_LABEL: Record<Stage, string> = {
  idle: "ready; press run to start the demo",
  submitting: "conduit submit: 300 messages onto three queues, faults on",
  draining: "workers polling, claiming keys, retrying and dead-lettering",
  clearing: "queues drained; DELETE /_faults on the webhook fake",
  replaying: "conduit dlq replay: dead letters back onto the work queue",
  done: "run complete; every README figure reproduced",
};

const STEPS: { stage: Stage; label: string }[] = [
  { stage: "submitting", label: "submit" },
  { stage: "draining", label: "drain" },
  { stage: "clearing", label: "clear fault" },
  { stage: "replaying", label: "replay" },
  { stage: "done", label: "summary" },
];

interface Row {
  name: string;
  type: string;
  queued: number;
  inFlight: number;
  delivered: number;
  dedup: number;
  retried: number;
  dlq: number;
  quarantine: number;
  waits: number;
}

function rows(engine: Engine): Row[] {
  return Object.keys(engine.specs).map((name) => {
    const rt = engine.connectors[name];
    const depth = rt.queue.depth();
    return {
      name,
      type: rt.spec.type,
      queued: depth.visible,
      inFlight: depth.notVisible,
      delivered: rt.target.inbox.length,
      dedup: rt.worker.stats.deduplicated,
      retried: rt.worker.stats.retried,
      dlq: rt.dlq.messages.length,
      quarantine: rt.quarantine.messages.length,
      waits: rt.worker.stats.rateLimitWaits,
    };
  });
}

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export function Run() {
  const reduced = useReducedMotion();
  const engine = useMemo(() => new Engine("D0D3904"), []);
  const [stage, setStage] = useState<Stage>("idle");
  const [table, setTable] = useState<Row[]>(() => rows(engine));
  const [events, setEvents] = useState<LogEvent[]>([]);
  const [clock, setClock] = useState(0);
  const [submitted, setSubmitted] = useState(0);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [report, setReport] = useState<CheckReport | null>(null);
  const cancel = useRef(false);

  const planDiff = useMemo(() => diffPlans(plan(CONNECTOR_YAML), plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML })), []);

  const refresh = useCallback(() => {
    setTable(rows(engine));
    setEvents(engine.log.slice(-120));
    setClock(engine.clock.now());
  }, [engine]);

  const drain = useCallback(async () => {
    const started = engine.clock.now();
    let idle = 0;
    while (!cancel.current && idle < 3) {
      const traces = await engine.tick(0.1);
      refresh();
      if (!traces.length && engine.totalQueued() === 0) idle += 1;
      else idle = 0;
      await wait(reduced ? 0 : 45);
    }
    return engine.clock.now() - started;
  }, [engine, refresh, reduced]);

  const run = useCallback(async () => {
    cancel.current = false;
    setSummary(null);
    setReport(null);
    engine.reset();
    refresh();
    setStage("submitting");
    const setup: ScenarioSetup = await setupScenario(engine);
    setSubmitted(setup.submitted);
    refresh();
    await wait(reduced ? 0 : 700);
    if (cancel.current) return;

    setStage("draining");
    const one = capturePhaseOne(engine, await drain());
    if (cancel.current) return;

    setStage("clearing");
    engine.connectors["webhook-crm"].target.clearFaults();
    engine.emit({ connector: "webhook-crm", kind: "info", event: "faults.cleared", taskId: "", key: "", detail: "DELETE /_faults on the webhook fake" });
    refresh();
    await wait(reduced ? 0 : 800);
    if (cancel.current) return;

    setStage("replaying");
    const replayed = engine.replay("webhook-crm").length;
    refresh();
    await wait(reduced ? 0 : 400);
    await drain();
    if (cancel.current) return;

    setSummary(summarize(engine, setup, one, replayed));
    setStage("done");
    setReport(await selfCheck());
  }, [engine, refresh, drain, reduced]);

  useEffect(() => () => {
    cancel.current = true;
  }, []);

  const running = stage !== "idle" && stage !== "done";
  const totals = table.reduce(
    (acc, r) => ({
      queued: acc.queued + r.queued + r.inFlight,
      delivered: acc.delivered + r.delivered,
      dedup: acc.dedup + r.dedup,
      retried: acc.retried + r.retried,
      dlq: acc.dlq + r.dlq,
    }),
    { queued: 0, delivered: 0, dedup: 0, retried: 0, dlq: 0 },
  );
  const stepIndex = STEPS.findIndex((s) => s.stage === stage);
  const summaryText = summary ? formatSummary(summary, planDiff.add.map((r) => r.address), planDiff.summary) : "";

  return (
    <section className="section" id="run" aria-labelledby="run-title">
      <div className="wrap">
        <SectionHead
          id="run-title"
          eyebrow="make demo"
          title={<>The whole run, and the numbers it prints</>}
          lede={
            <>
              <code>make demo</code> submits 300 tasks across the three connectors with faults on, drains the queues, clears the
              webhook fault, replays the dead letters, and plans a fourth connector. This is the same run, driven a tenth of a
              second at a time, ending in the summary block the README quotes.
            </>
          }
        />

        <Reveal delay={0.05}>
          <div className="run-controls">
            <button className="btn btn-primary" onClick={() => void run()} disabled={running}>
              {running ? "Running" : summary ? "Run again" : "Run make demo"}
            </button>
            <ol className="run-steps" aria-label="Run progress">
              {STEPS.map((s, i) => (
                <li key={s.stage} className={`run-step ${stepIndex >= i && stepIndex >= 0 ? "run-step-on" : ""} ${stage === s.stage ? "run-step-now" : ""}`}>
                  <span className="run-step-dot" aria-hidden />
                  {s.label}
                </li>
              ))}
            </ol>
            <span className="run-clock mono">
              seed D0D3904 <span aria-hidden>·</span> t+{clock.toFixed(1)}s
            </span>
          </div>
          <p className="run-stage" role="status" aria-live="polite">
            {STAGE_LABEL[stage]}
          </p>
        </Reveal>

        <Reveal delay={0.1}>
          <dl className="run-counters glass" aria-live="polite" aria-atomic="false">
            <div className="stat">
              <dt className="stat-label">submitted</dt>
              <dd className="stat-value"><Counter value={submitted} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">in the queues</dt>
              <dd className="stat-value"><Counter value={totals.queued} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">delivered</dt>
              <dd className="stat-value stat-teal"><Counter value={totals.delivered} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">deduplicated</dt>
              <dd className="stat-value stat-dedup"><Counter value={totals.dedup} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">retried</dt>
              <dd className="stat-value stat-warn"><Counter value={totals.retried} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">in the DLQ</dt>
              <dd className={`stat-value ${totals.dlq > 0 ? "stat-bad" : "stat-ok"}`}><Counter value={totals.dlq} /></dd>
            </div>
          </dl>
        </Reveal>

        <div className="run-grid">
          <Reveal delay={0.12} className="run-panel glass">
            <span className="code-label">queues and workers</span>
            <div className="run-table-scroll">
              <table className="table run-table">
                <thead>
                  <tr>
                    <th scope="col">connector</th>
                    <th scope="col">queued</th>
                    <th scope="col">in flight</th>
                    <th scope="col">delivered</th>
                    <th scope="col">dedup</th>
                    <th scope="col">retried</th>
                    <th scope="col">paced</th>
                    <th scope="col">DLQ</th>
                    <th scope="col">quarantine</th>
                  </tr>
                </thead>
                <tbody>
                  {table.map((r) => (
                    <tr key={r.name}>
                      <th scope="row">
                        <Chip tone={TYPE_TONE[r.type]}>
                          <span className="dot" /> {r.name}
                        </Chip>
                      </th>
                      <td className="mono">{r.queued}</td>
                      <td className="mono">{r.inFlight}</td>
                      <td className="mono run-good">{r.delivered}</td>
                      <td className="mono run-dedup">{r.dedup}</td>
                      <td className="mono run-warn">{r.retried}</td>
                      <td className="mono">{r.waits}</td>
                      <td className={`mono ${r.dlq ? "run-bad" : ""}`}>{r.dlq}</td>
                      <td className={`mono ${r.quarantine ? "run-bad" : ""}`}>{r.quarantine}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="run-note">
              <b>paced</b> counts sends the per-connector token bucket held back: jira runs at 10 requests a second with a burst of
              one, slack at 20, the webhook receiver at 50. A 429 with <code>Retry-After</code> pauses every send on that
              connector, not just the message that was throttled.
            </p>
          </Reveal>

          <Reveal delay={0.16} className="run-panel glass run-log">
            <LogStream events={events} limit={200} label="worker log (structlog, JSON)" />
          </Reveal>
        </div>

        <AnimatePresence>
          {summary ? (
            <motion.div
              className="run-summary"
              initial={reduced ? false : { opacity: 0, y: 18 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
            >
              <div className="run-summary-head">
                <span className="eyebrow">demo summary</span>
                <Chip tone={summary.ok ? "chip-teal" : "chip-bad"}>
                  <span className="dot" /> {summary.ok ? "all checks passed" : "checks failed"}
                </Chip>
                {report ? (
                  <Chip tone={report.ok ? "chip-teal" : "chip-bad"}>
                    {report.passed}/{report.assertions} assertions
                  </Chip>
                ) : null}
              </div>
              <pre className="code run-summary-block" tabIndex={0} aria-label="Demo summary block">
                <code>{summaryText}</code>
              </pre>
              <p className="run-note">
                Every figure above is read back from the in-memory queues, the fake targets' inboxes (which record idempotency
                keys), the worker stats, and the derived Terraform plan. The same assertions run headless in Node through
                <code> npm run selfcheck</code>.
              </p>
            </motion.div>
          ) : null}
        </AnimatePresence>
      </div>
    </section>
  );
}
