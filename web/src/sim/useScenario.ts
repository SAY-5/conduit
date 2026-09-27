// One description of the demo's choreography, shared by the hero, the run section and the
// dead-letter lab: submit with faults on, drain, clear the webhook fault, replay, drain again.
// The three differ only in how fast they step and what they draw between steps.

import { useCallback, useEffect, useRef, useState } from "react";
import { capturePhaseOne, setupScenario, summarize, type Engine, type PhaseOne, type ScenarioSetup, type Summary } from "./engine";
import type { QueueMessage } from "./queue";
import type { HandleTrace } from "./worker";

export type ScenarioPhase = "idle" | "submitting" | "draining" | "clearing" | "replaying" | "done";

/** Wall-clock milliseconds between simulated ticks and between stages. */
export interface ScenarioSpeed {
  tick: number;
  afterSubmit: number;
  afterDrain: number;
  afterReplay: number;
}

export interface ScenarioOptions {
  speed: ScenarioSpeed;
  /** Slowed to nothing when the viewer asks for reduced motion. */
  reduced?: boolean;
  /** Every simulated tick, with the traces it produced. */
  onTick?: (traces: HandleTrace[]) => void;
  /** The messages a replay moved back onto the work queue. */
  onReplay?: (moved: QueueMessage[]) => void;
}

export interface Scenario {
  phase: ScenarioPhase;
  running: boolean;
  submitted: number;
  setup: ScenarioSetup | null;
  phaseOne: PhaseOne | null;
  summary: Summary | null;
  /** Run the whole scenario from a reset engine. */
  run: () => Promise<void>;
  /** Poll until the queues stay empty; returns the simulated seconds it took. */
  drain: () => Promise<number>;
  /** Clear the webhook fake's faults, logging it as the demo does. */
  clearFault: () => void;
  /** Move the dead letters back onto the work queue. */
  replay: () => QueueMessage[];
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export function useScenario(engine: Engine, options: ScenarioOptions): Scenario {
  const { speed, reduced = false, onTick, onReplay } = options;
  const [phase, setPhase] = useState<ScenarioPhase>("idle");
  const [submitted, setSubmitted] = useState(0);
  const [setup, setSetup] = useState<ScenarioSetup | null>(null);
  const [phaseOne, setPhaseOne] = useState<PhaseOne | null>(null);
  const [summary, setSummary] = useState<Summary | null>(null);
  const cancelled = useRef(false);
  const tickRef = useRef(onTick);
  const replayRef = useRef(onReplay);
  tickRef.current = onTick;
  replayRef.current = onReplay;

  useEffect(
    () => () => {
      cancelled.current = true;
    },
    [],
  );

  const pause = useCallback(async (ms: number) => sleep(reduced ? Math.min(ms, 8) : ms), [reduced]);

  const drain = useCallback(async () => {
    const started = engine.clock.now();
    let idle = 0;
    while (!cancelled.current && idle < 3) {
      const traces = await engine.tick(0.1);
      tickRef.current?.(traces);
      if (!traces.length && engine.totalQueued() === 0) idle += 1;
      else idle = 0;
      await sleep(reduced ? 0 : speed.tick);
    }
    return engine.clock.now() - started;
  }, [engine, reduced, speed.tick]);

  const clearFault = useCallback(() => engine.clearFaults("webhook-crm"), [engine]);

  const replay = useCallback(() => {
    const moved = engine.replay("webhook-crm");
    replayRef.current?.(moved);
    return moved;
  }, [engine]);

  const run = useCallback(async () => {
    cancelled.current = false;
    setSummary(null);
    setPhaseOne(null);
    engine.reset();
    setSubmitted(0);
    tickRef.current?.([]);
    setPhase("submitting");
    const fresh = await setupScenario(engine);
    setSetup(fresh);
    setSubmitted(fresh.submitted);
    tickRef.current?.([]);
    await pause(speed.afterSubmit);
    if (cancelled.current) return;

    setPhase("draining");
    const one = capturePhaseOne(engine, await drain());
    setPhaseOne(one);
    if (cancelled.current) return;

    setPhase("clearing");
    clearFault();
    tickRef.current?.([]);
    await pause(speed.afterDrain);
    if (cancelled.current) return;

    setPhase("replaying");
    const replayed = replay().length;
    tickRef.current?.([]);
    await pause(speed.afterReplay);
    await drain();
    if (cancelled.current) return;

    setSummary(summarize(engine, fresh, one, replayed));
    setPhase("done");
  }, [engine, drain, pause, clearFault, replay, speed.afterSubmit, speed.afterDrain, speed.afterReplay]);

  const running = phase !== "idle" && phase !== "done";
  return { phase, running, submitted, setup, phaseOne, summary, run, drain, clearFault, replay };
}
