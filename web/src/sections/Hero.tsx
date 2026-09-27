import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Chip, Counter, TYPE_TONE } from "../components/common";
import { LogStream } from "../components/LogStream";
import { Engine, SCENARIO_MESSAGES } from "../sim/engine";
import type { QueueMessage } from "../sim/queue";
import { useScenario, type ScenarioPhase } from "../sim/useScenario";
import { CONNECTOR_YAML, NEW_CONNECTOR_YAML } from "../sim/specs";
import { diffPlans, plan } from "../sim/terraform";
import type { HandleTrace, LogEvent } from "../sim/worker";
import "./hero.css";

interface LaneSnap {
  name: string;
  type: string;
  queued: number;
  inbox: number;
  dedup: number;
  retried: number;
  dlq: number;
  quarantine: number;
  target: string;
}

interface Particle {
  id: number;
  lane: number;
  kind: "ok" | "retry" | "dedup" | "bounce" | "dlq" | "replay";
}

const LANES = ["slack-ops", "jira-support", "webhook-crm"] as const;
const TARGET_LABEL: Record<string, string> = { "slack-ops": "chat.postMessage", "jira-support": "POST /rest/api/3/issue", "webhook-crm": "signed POST" };
const TICK_MS = 55;

function snapshot(engine: Engine): LaneSnap[] {
  return LANES.map((name) => {
    const rt = engine.connectors[name];
    return {
      name,
      type: rt.spec.type,
      queued: rt.queue.messages.length,
      inbox: rt.target.inbox.length,
      dedup: rt.worker.stats.deduplicated,
      retried: rt.worker.stats.retried,
      dlq: rt.dlq.messages.length,
      quarantine: rt.quarantine.messages.length,
      target: rt.spec.target,
    };
  });
}

function kindOf(t: HandleTrace): Particle["kind"] {
  if (t.reason === "delivered") return t.message.replays > 0 ? "replay" : t.retry && t.retry.attempts > 1 ? "retry" : "ok";
  if (t.reason === "deduplicated") return "dedup";
  return t.willDeadLetter ? "dlq" : "bounce";
}

export function Hero() {
  const reduced = useReducedMotion();
  const engine = useMemo(() => new Engine("D0D3904"), []);
  const [lanes, setLanes] = useState<LaneSnap[]>(() => snapshot(engine));
  const [particles, setParticles] = useState<Particle[]>([]);
  const [events, setEvents] = useState<LogEvent[]>([]);
  const [clock, setClock] = useState(0);
  const pid = useRef(0);

  const planDiff = useMemo(() => diffPlans(plan(CONNECTOR_YAML), plan({ ...CONNECTOR_YAML, "pager-oncall": NEW_CONNECTOR_YAML })), []);

  const refresh = useCallback(() => {
    setLanes(snapshot(engine));
    setEvents(engine.log.slice(-60));
    setClock(engine.clock.now());
  }, [engine]);

  const spawn = useCallback(
    (traces: HandleTrace[]) => {
      if (reduced) return;
      const fresh = traces.map((t) => ({ id: ++pid.current, lane: LANES.indexOf(t.message.envelope.connector as (typeof LANES)[number]), kind: kindOf(t) }));
      if (!fresh.length) return;
      setParticles((p) => [...p.slice(-40), ...fresh]);
      const ids = new Set(fresh.map((f) => f.id));
      setTimeout(() => setParticles((p) => p.filter((x) => !ids.has(x.id))), 1100);
    },
    [reduced],
  );

  const onTick = useCallback(
    (traces: HandleTrace[]) => {
      spawn(traces);
      refresh();
    },
    [spawn, refresh],
  );

  const onReplay = useCallback(
    (moved: QueueMessage[]) => spawn(moved.map((m) => ({ message: m, reason: "delivered", retry: null, claim: { acquired: true, state: null, remoteId: null, condition: "attribute_not_exists" }, result: null, elapsed: 0, willDeadLetter: false }))),
    [spawn],
  );

  const scenario = useScenario(engine, { speed: { tick: TICK_MS, afterSubmit: 900, afterDrain: 1100, afterReplay: 500 }, reduced: !!reduced, onTick, onReplay });
  const { phase, running, submitted } = scenario;

  useEffect(() => {
    const t = setTimeout(() => void scenario.run(), 700);
    return () => clearTimeout(t);
  }, [scenario.run]);

  const totalDedup = lanes.reduce((n, l) => n + l.dedup, 0);
  const totalDlq = lanes.reduce((n, l) => n + l.dlq, 0);
  const totalDelivered = lanes.reduce((n, l) => n + l.inbox, 0);
  const totalQuarantined = lanes.reduce((n, l) => n + l.quarantine, 0);
  const totalQueued = lanes.reduce((n, l) => n + l.queued, 0);
  const totalRetried = lanes.reduce((n, l) => n + l.retried, 0);
  const phaseLabel: Record<ScenarioPhase, string> = {
    idle: "ready",
    submitting: `${SCENARIO_MESSAGES} messages queued, faults on`,
    draining: "workers polling",
    clearing: "queues drained; clearing the webhook fault",
    replaying: "replaying dead letters",
    done: "all checks passed",
  };

  return (
    <section className="hero" aria-labelledby="hero-title">
      <div className="wrap hero-grid">
        <div className="hero-copy">
          <motion.span className="eyebrow" initial={reduced ? false : { opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.1 }}>
            conduit / integration connector kit
          </motion.span>
          <motion.h1 id="hero-title" className="hero-title" initial={reduced ? false : { opacity: 0, y: 26 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.9, ease: [0.22, 1, 0.36, 1] }}>
            One adapter interface.
            <br />
            Three targets.
            <br />
            <span className="hero-accent">Nothing delivered twice.</span>
          </motion.h1>
          <motion.p className="hero-lede" initial={reduced ? false : { opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.8, delay: 0.15 }}>
            Slack, Jira, and signed webhooks behind one <code>deliver()</code> contract, with sha256 idempotency keys claimed by conditional put, full-jitter backoff, and an SQS dead-letter queue per connector. This page is a TypeScript port of the Python kit, running the README demo in your browser.
          </motion.p>
          <motion.dl className="hero-stats" initial={reduced ? false : { opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.8, delay: 0.3 }}>
            <div className="stat">
              <dt className="stat-label">tasks submitted</dt>
              <dd className="stat-value"><Counter value={submitted} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">deduplicated</dt>
              <dd className="stat-value stat-dedup"><Counter value={totalDedup} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">dead-lettered</dt>
              <dd className={`stat-value ${totalDlq > 0 ? "stat-bad" : "stat-ok"}`}><Counter value={totalDlq} /></dd>
            </div>
            <div className="stat">
              <dt className="stat-label">resources per YAML</dt>
              <dd className="stat-value stat-teal"><Counter value={planDiff.add.length} /></dd>
            </div>
          </motion.dl>
          <div className="hero-actions">
            <button className="btn btn-primary" onClick={() => void scenario.run()} disabled={running}>
              {running ? "Running" : "Run the demo again"}
            </button>
            <a className="btn" href="#interface">Read the interface</a>
            <span className="hero-phase" role="status">
              <span className={`dot ${running ? "dot-live" : ""}`} /> {phaseLabel[phase]} <span className="mono">t+{clock.toFixed(1)}s</span>
            </span>
          </div>
        </div>

        <motion.div className="board glass" initial={reduced ? false : { opacity: 0, scale: 0.97 }} animate={{ opacity: 1, scale: 1 }} transition={{ duration: 1, delay: 0.25, ease: [0.22, 1, 0.36, 1] }} aria-label="Tasks fanning out through per-connector queues into Slack, Jira, and a webhook receiver">
          <div className="board-head">
            <span className="mono">conduit submit tasks.json -c &lt;connector&gt;</span>
            <span className="board-totals mono">
              queued {totalQueued} / delivered {totalDelivered} / retried {totalRetried} / quarantined {totalQuarantined}
            </span>
          </div>
          <div className="board-body">
            <div className="board-source">
              <div className="source-card">
                <span className="code-label">submit</span>
                <strong className="mono">{submitted}</strong>
                <span className="source-sub">240 unique + 60 resubmits + 2 malformed</span>
              </div>
              <div className="source-card source-faults">
                <span className="code-label">faults</span>
                <span className={`fault ${phase === "done" || phase === "replaying" || phase === "idle" ? "fault-off" : ""}`}>jira 429 x2 for 30 tasks</span>
                <span className={`fault ${phase === "done" || phase === "replaying" || phase === "idle" ? "fault-off" : ""}`}>webhook 400 for 10 tasks</span>
                <span className="fault fault-quarantine">2 payloads their schema rejects</span>
              </div>
            </div>
            <div className="board-lanes">
              {lanes.map((lane, i) => (
                <div className={`lane lane-${lane.type}`} key={lane.name}>
                  <div className="queue-box">
                    <span className="queue-name mono">conduit-{lane.name}</span>
                    <div className="queue-bar" aria-hidden>
                      <motion.span className="queue-fill" animate={{ width: `${Math.min(100, lane.queued)}%` }} transition={{ duration: reduced ? 0 : 0.2 }} />
                    </div>
                    <span className="queue-depth mono">{lane.queued} visible</span>
                    {lane.name === "webhook-crm" ? (
                      <div className={`dlq-box ${lane.dlq > 0 ? "dlq-hot" : ""}`}>
                        <span className="mono">conduit-webhook-crm-dlq</span>
                        <strong className="mono">{lane.dlq}</strong>
                      </div>
                    ) : null}
                  </div>
                  <div className="wire" aria-hidden>
                    <span className="wire-line" />
                    {particles.filter((p) => p.lane === i).map((p) => (
                      <span key={p.id} className={`particle particle-${p.kind}`} />
                    ))}
                  </div>
                  <div className="target-card">
                    <div className="target-top">
                      <Chip tone={TYPE_TONE[lane.type]}>
                        <span className="dot" /> {lane.type}
                      </Chip>
                      <span className="mono target-target">{lane.target.replace("https://", "")}</span>
                    </div>
                    <div className="target-inbox">
                      <strong className="mono">{lane.inbox}</strong>
                      <span>in inbox</span>
                      <div className="target-meta mono">
                        <span>{lane.dedup} dedup</span>
                        {lane.retried ? <span>{lane.retried} retries</span> : null}
                      </div>
                    </div>
                    <div className="target-meta mono">
                      <span>{TARGET_LABEL[lane.name]}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div className="board-foot">
            <LogStream events={events} limit={7} label="worker logs" />
            <AnimatePresence>
              {phase === "done" ? (
                <motion.div className="board-plan mono" initial={reduced ? false : { opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
                  <span className="add">+ connectors/pager-oncall.yaml</span>
                  <span>{planDiff.summary}</span>
                </motion.div>
              ) : null}
            </AnimatePresence>
          </div>
        </motion.div>
      </div>
      <div className="hero-legend wrap mono">
        <span><i className="particle-swatch particle-ok" aria-hidden /> delivered</span>
        <span><i className="particle-swatch particle-retry" aria-hidden /> delivered after 429 retries</span>
        <span><i className="particle-swatch particle-dedup" aria-hidden /> deduplicated</span>
        <span><i className="particle-swatch particle-bounce" aria-hidden /> failed, redelivered</span>
        <span><i className="particle-swatch particle-dlq" aria-hidden /> dead-lettered</span>
        <span><i className="particle-swatch particle-replay" aria-hidden /> replayed</span>
      </div>
    </section>
  );
}
