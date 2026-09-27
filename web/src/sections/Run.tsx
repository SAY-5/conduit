import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Chip, Counter, Reveal, SectionHead, TYPE_TONE } from "../components/common";
import { LogStream } from "../components/LogStream";
import { Engine, formatSummary } from "../sim/engine";
import { selfCheck, type CheckReport } from "../sim/selfcheck";
import { CONNECTOR_YAML, NEW_CONNECTOR_YAML } from "../sim/specs";
import { diffPlans, plan } from "../sim/terraform";
import { useScenario, type ScenarioPhase } from "../sim/useScenario";
import type { LogEvent } from "../sim/worker";
import "./run.css";

const STAGE_LABEL: Record<ScenarioPhase, string> = {
  idle: "ready; press run to start the demo",
  submitting: "conduit submit: 302 messages onto three queues, faults on",
  draining: "workers polling, claiming keys, retrying and dead-lettering",
  clearing: "queues drained; DELETE /_faults on the webhook fake",
  replaying: "conduit dlq replay: dead letters back onto the work queue",
  done: "run complete; every README figure reproduced",
};

const STEPS: { stage: ScenarioPhase; label: string }[] = [
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

export function Run() {
  const reduced = useReducedMotion();
  const engine = useMemo(() => new Engine("D0D3904"), []);
  const [table, setTable] = useState<Row[]>(() => rows(engine));
  const [events, setEvents] = useState<LogEvent[]>([]);
  const [clock, setClock] = useState(0);
  const [report, setReport] = useState<CheckReport | null>(null);

  const planDiff = useMemo(() => diffPlans(plan(CONNECTOR_YAML), plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML })), []);
  const pacing = useMemo(
    () => Object.values(engine.specs).map((spec) => `${spec.name} ${spec.rateLimit.requestsPerSecond} a second with a burst of ${spec.rateLimit.burst}`).join(", "),
    [engine],
  );

  const refresh = useCallback(() => {
    setTable(rows(engine));
    setEvents(engine.log.slice(-120));
    setClock(engine.clock.now());
  }, [engine]);
  const onTick = useCallback(() => refresh(), [refresh]);

  const scenario = useScenario(engine, { speed: { tick: 45, afterSubmit: 700, afterDrain: 800, afterReplay: 400 }, reduced: !!reduced, onTick });
  const { phase, running, submitted, setup, phaseOne, summary } = scenario;

  const run = useCallback(async () => {
    setReport(null);
    await scenario.run();
  }, [scenario]);

  // The assertions read the run that just finished on screen instead of replaying a third
  // scenario on the main thread.
  useEffect(() => {
    if (phase !== "done" || !summary || !setup || !phaseOne) return;
    let live = true;
    void selfCheck({ engine, setup, one: phaseOne, summary }).then((r) => live && setReport(r));
    return () => {
      live = false;
    };
  }, [phase, summary, setup, phaseOne, engine]);

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
  const stepIndex = STEPS.findIndex((s) => s.stage === phase);
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
              <code>make demo</code> submits 302 tasks across the three connectors with faults on: 240 unique, 60 resubmits, and
              two payloads their source schema rejects. It drains the queues, clears the webhook fault, replays the dead letters,
              and plans a fourth connector. This is the same run, driven a tenth of a second at a time, ending in the delivery
              half of the summary block the README quotes; the ops and cost tables the README also prints need a running worker
              and a price list, so they are not reproduced here.
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
                <li key={s.stage} className={`run-step ${stepIndex >= i && stepIndex >= 0 ? "run-step-on" : ""} ${phase === s.stage ? "run-step-now" : ""}`}>
                  <span className="run-step-dot" aria-hidden />
                  {s.label}
                </li>
              ))}
            </ol>
            <span className="run-clock mono">
              seed D0D3904 <span aria-hidden>·</span> t+{clock.toFixed(1)}s
            </span>
          </div>
          <p className="run-stage" role="status">
            {STAGE_LABEL[phase]}
          </p>
        </Reveal>

        <Reveal delay={0.1}>
          <dl className="run-counters glass">
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
              <b>paced</b> counts sends the per-connector token bucket held back, at the rates the shipped YAMLs set:{" "}
              {pacing}. At this pace the bursts absorb the run, so the column stays at zero and the only thing that moves the
              earliest allowed send is a 429 with <code>Retry-After</code>, which pauses every send on that connector rather
              than only the message that was throttled.
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
