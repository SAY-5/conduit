import { useEffect, useRef } from "react";
import type { LogEvent } from "../sim/worker";

const KIND_CLASS: Record<string, string> = {
  ok: "log-ok",
  dedup: "log-dedup",
  retry: "log-retry",
  fail: "log-fail",
  dlq: "log-dlq",
  quarantine: "log-fail",
  replay: "log-replay",
  claim: "log-claim",
  in_progress: "log-retry",
  submit: "log-claim",
  info: "log-claim",
  throttle: "log-throttle",
  breaker: "log-breaker",
};

export function LogStream({ events, limit = 80, follow = true, label = "worker log (structlog, JSON)" }: { events: LogEvent[]; limit?: number; follow?: boolean; label?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const shown = events.slice(-limit);
  useEffect(() => {
    if (follow && ref.current) ref.current.scrollTop = ref.current.scrollHeight;
  }, [events.length, follow]);
  return (
    <div className="log-wrap">
      <span className="code-label">{label}</span>
      <div className="log" ref={ref} role="log" aria-live="polite" aria-label={label}>
        {shown.length === 0 ? <div className="log-line"><span className="t">--</span><span className="ev">idle</span><span>waiting for messages</span></div> : null}
        {shown.map((e) => (
          <div className={`log-line ${KIND_CLASS[e.kind] ?? ""}`} key={e.seq}>
            <span className="t">t+{e.t.toFixed(2)}s</span>
            <span className="ev">{e.event.replace("delivery.", "")}</span>
            <span>
              {e.connector}
              {e.taskId ? ` ${e.taskId.replace(/^[A-Z0-9]+-/, "")}` : ""}
              {e.key ? ` key=${e.key}` : ""} {e.detail}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
