import { useEffect, useMemo, useState } from "react";
import { Chip, Code, Reveal, SectionHead, TYPE_TONE } from "../components/common";
import { buildAdapter, FakeTarget, type RenderedRequest } from "../sim/adapters";
import { idempotencyKey, keyMaterial } from "../sim/idempotency";
import { makeTask } from "../sim/models";
import { Rng } from "../sim/prng";
import { CONNECTOR_NAMES, CONNECTOR_YAML, dlqName, loadAll, quarantineName, queueName } from "../sim/specs";
import "./interface.css";

const ADAPTER_PY = `class Adapter(abc.ABC):
    """Deliver one task revision to a remote system, idempotently."""

    def deliver(self, task: Task, idempotency_key: str,
                remote_id: str | None = None) -> DeliveryResult: ...

    def healthcheck(self) -> bool: ...

    # raises TransientError (429, 5xx, timeout) or PermanentError (other 4xx)
    # never sleeps, never loops, never touches the queue`;

const SPECS = loadAll();

export function Interface() {
  const [name, setName] = useState<string>("jira-support");
  const [taskId, setTaskId] = useState("TASK-1042");
  const [version, setVersion] = useState(1);
  const [title, setTitle] = useState("Payment webhook retries exhausted for tenant acme");
  const [priority, setPriority] = useState("high");
  const [key, setKey] = useState("");
  const [request, setRequest] = useState<RenderedRequest | null>(null);
  const spec = SPECS[name];
  const material = keyMaterial(name, taskId || "task", version);
  const task = useMemo(
    () => makeTask({ id: taskId || "task", version, title, body: "Three attempts returned 502 from the PSP; the DLQ has 4 messages.", priority, status: "open", assignee: "m.okafor", labels: ["billing", "incident"], url: "https://tasks.example.com/TASK-1042" }),
    [taskId, version, title, priority],
  );
  const adapter = useMemo(() => buildAdapter(spec, new FakeTarget(spec.type, new Rng(name), () => 0), () => 0), [spec, name]);

  useEffect(() => {
    let live = true;
    idempotencyKey(name, task.id, task.version).then((k) => live && setKey(k));
    adapter.render(task, "<idempotency key>", version > 1 && spec.type === "jira" ? "SUP-1041" : null).then((r) => live && setRequest(r));
    return () => {
      live = false;
    };
  }, [name, task, adapter, version, spec.type]);

  const shownRequest = request ? { ...request, headers: Object.fromEntries(Object.entries(request.headers).map(([k, v]) => [k, v.replace("<idempotency key>", key)])) , body: JSON.parse(JSON.stringify(request.body).replaceAll("<idempotency key>", key)) } : null;

  return (
    <section className="section" id="interface" aria-labelledby="interface-title">
      <div className="wrap">
        <SectionHead eyebrow="01 / one interface" id="interface-title" title="Pick a connector YAML. The worker and Terraform both read it." lede="Every integration is one ConnectorSpec: type, target, secret names, field mapping, retry policy, rate limit, and queue settings. The adapter built from it implements a single deliver() and never sleeps, loops, or touches the queue." />

        <Reveal className="iface-grid" delay={0.1}>
          <div className="iface-left">
            <div className="seg" role="group" aria-label="Connector">
              {CONNECTOR_NAMES.map((n) => (
                <button key={n} aria-pressed={n === name} onClick={() => setName(n)}>
                  {n}
                </button>
              ))}
            </div>
            <Code lang="yaml" label={`connectors/${name}.yaml`} text={CONNECTOR_YAML[name].trim()} />
            <Code lang="py" label="conduit/adapters/base.py" text={ADAPTER_PY} />
          </div>

          <div className="iface-right">
            <div className="glass spec-card">
              <div className="spec-head">
                <span className="code-label">ConnectorSpec (pydantic, validated)</span>
                <Chip tone={TYPE_TONE[spec.type]}>
                  <span className="dot" /> {spec.type}
                </Chip>
              </div>
              <dl className="spec-list">
                <div><dt>name</dt><dd className="mono">{spec.name}</dd></div>
                <div><dt>target</dt><dd className="mono">{spec.target}</dd></div>
                <div><dt>base_url</dt><dd className="mono">{spec.baseUrl ?? "(none)"}</dd></div>
                <div>
                  <dt>secrets</dt>
                  <dd className="mono">{Object.entries(spec.secrets).map(([l, e]) => <span key={l} className="kv"><b>{l}</b> ← ${e}</span>)}</dd>
                </div>
                <div>
                  <dt>mapping</dt>
                  <dd className="mono">{Object.entries(spec.mapping).map(([r, s]) => <span key={r} className="kv"><b>{r}</b> ← {s}</span>)}</dd>
                </div>
                <div>
                  <dt>retry</dt>
                  <dd className="mono">max_attempts {spec.retry.maxAttempts} · base {spec.retry.baseSeconds}s · cap {spec.retry.maxSeconds}s · x{spec.retry.multiplier} full jitter · retry on {spec.retry.retryOnStatus.join(" ")} and 5xx</dd>
                </div>
                <div><dt>rate_limit</dt><dd className="mono">{spec.rateLimit.requestsPerSecond} requests/s</dd></div>
                <div>
                  <dt>queue</dt>
                  <dd className="mono">{queueName(spec)} → {dlqName(spec)} after <b>maxReceiveCount={spec.queue.maxReceiveCount}</b> · schema failures → {quarantineName(spec)} · visibility {spec.queue.visibilityTimeoutSeconds}s</dd>
                </div>
                <div><dt>idempotency ttl</dt><dd className="mono">{spec.idempotencyTtlSeconds}s</dd></div>
              </dl>
            </div>

            <div className="glass spec-card">
              <span className="code-label">adapter.deliver(task, idempotency_key{version > 1 ? ", remote_id" : ""})</span>
              <div className="task-form">
                <div className="field"><label htmlFor="t-id">task.id</label><input id="t-id" className="input" value={taskId} onChange={(e) => setTaskId(e.target.value.slice(0, 64))} /></div>
                <div className="field"><label htmlFor="t-ver">task.version</label><input id="t-ver" className="input" type="number" min={1} max={99} value={version} onChange={(e) => setVersion(Math.max(1, Number(e.target.value) || 1))} /></div>
                <div className="field field-wide"><label htmlFor="t-title">task.title</label><input id="t-title" className="input" value={title} onChange={(e) => setTitle(e.target.value.slice(0, 120))} /></div>
                <div className="field"><label htmlFor="t-pri">task.priority</label>
                  <select id="t-pri" className="select" value={priority} onChange={(e) => setPriority(e.target.value)}>
                    {["low", "normal", "high", "urgent"].map((p) => <option key={p}>{p}</option>)}
                  </select>
                </div>
              </div>
              <div className="key-derivation mono">
                <span className="code-label">idempotency key</span>
                <span className="key-material">sha256("<b>{material}</b>")</span>
                <span className="key-arrow" aria-hidden>↓</span>
                <span className="key-hex">{key || "computing"}</span>
              </div>
              {shownRequest ? (
                <Code
                  lang="json"
                  label={`${shownRequest.method} ${shownRequest.url}`}
                  text={`${Object.entries(shownRequest.headers).map(([k, v]) => `${k}: ${v}`).join("\n")}\n\n${JSON.stringify(shownRequest.body, null, 2)}`}
                />
              ) : null}
              {version > 1 ? <p className="spec-note">Revision {version}: the worker resolves the remote id recorded for revision {version - 1} and the adapter updates instead of creating{spec.type === "jira" ? " (PUT /issue/SUP-1041; a 404 falls back to create)" : ""}.</p> : null}
            </div>
          </div>
        </Reveal>
      </div>
    </section>
  );
}
