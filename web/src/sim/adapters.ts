// Port of conduit/adapters/{base,slack,jira,webhook}.py plus the FastAPI fakes
// in fakes/: each adapter renders the request the real one sends, and each fake
// target records an inbox keyed by Idempotency-Key with runtime fault injection.

import { taskField, type DeliveryResult, type Task } from "./models";
import type { Rng } from "./prng";
import { classifyResponse, PermanentError, TransientError, type FakeResponse } from "./retry";
import type { ConnectorSpec } from "./specs";

export interface RenderedRequest {
  method: "POST" | "PUT";
  url: string;
  headers: Record<string, string>;
  body: Record<string, unknown>;
}

export interface FaultSpec {
  rateLimitTasks: Set<string>;
  rateLimitCount: number;
  hardFailTasks: Set<string>;
  fail500Once: boolean;
}

export interface InboxEntry {
  seq: number;
  taskId: string;
  version: number;
  idempotencyKey: string;
  remoteId: string;
  receivedAt: number;
  replayed: boolean;
}

/** One FastAPI-style fake: /_inbox, /_faults, and the fault injection of fakes/common.py. */
export class FakeTarget {
  faults: FaultSpec = { rateLimitTasks: new Set(), rateLimitCount: 2, hardFailTasks: new Set(), fail500Once: false };
  readonly inbox: InboxEntry[] = [];
  readonly rateLimitHits = new Map<string, number>();
  private fired500 = new Set<string>();
  calls = 0;
  rejected = 0;
  /** Simulated wall time per call, seconds. */
  lastLatency = 0;

  constructor(readonly type: ConnectorSpec["type"], readonly rng: Rng, readonly clock: () => number) {}

  setFaults(faults: Partial<FaultSpec>): void {
    this.faults = { ...this.faults, ...faults };
    this.rateLimitHits.clear();
    this.fired500.clear();
  }

  clearFaults(): void {
    this.setFaults({ rateLimitTasks: new Set(), hardFailTasks: new Set(), fail500Once: false });
  }

  clearInbox(): void {
    this.inbox.length = 0;
  }

  seenKeys(): Set<string> {
    return new Set(this.inbox.map((e) => e.idempotencyKey));
  }

  private inject(taskId: string): FakeResponse | null {
    this.calls += 1;
    const f = this.faults;
    if (f.hardFailTasks.has(taskId)) {
      this.rejected += 1;
      return { status: 400, headers: {}, body: { error: "invalid_payload", reason: "hard_fail" } };
    }
    if (f.rateLimitTasks.has(taskId)) {
      const hits = this.rateLimitHits.get(taskId) ?? 0;
      if (hits < f.rateLimitCount) {
        this.rateLimitHits.set(taskId, hits + 1);
        this.rejected += 1;
        return { status: 429, headers: { "Retry-After": "0" }, body: { error: "ratelimited", reason: "rate_limited" } };
      }
    }
    if (f.fail500Once && !this.fired500.has(taskId)) {
      this.fired500.add(taskId);
      this.rejected += 1;
      return { status: 500, headers: {}, body: { error: "internal_error", reason: "flaky" } };
    }
    return null;
  }

  /** Handle one request the way the fake app would; dedupes on Idempotency-Key like the webhook receiver. */
  handle(request: RenderedRequest, task: Task, replayed: boolean): FakeResponse {
    this.lastLatency = this.rng.uniform(0.0004, 0.0026);
    const fault = this.inject(task.id);
    if (fault) return fault;
    const key = request.headers["Idempotency-Key"];
    const existing = this.inbox.find((e) => e.idempotencyKey === key);
    if (existing) return this.ok(existing.remoteId);
    const remoteId = this.remoteId(task);
    this.inbox.push({
      seq: this.inbox.length + 1,
      taskId: task.id,
      version: task.version,
      idempotencyKey: key,
      remoteId,
      receivedAt: this.clock(),
      replayed,
    });
    return this.ok(remoteId);
  }

  private remoteId(task: Task): string {
    switch (this.type) {
      case "slack":
        return `${1700000000 + this.inbox.length}.${this.rng.hex(6)}`;
      case "jira":
        return `SUP-${1000 + this.inbox.length + 1}`;
      default:
        return `evt_${this.rng.hex(10)}`;
    }
  }

  private ok(remoteId: string): FakeResponse {
    switch (this.type) {
      case "slack":
        return { status: 200, headers: { "content-type": "application/json" }, body: { ok: true, ts: remoteId } };
      case "jira":
        return { status: 201, headers: { "content-type": "application/json" }, body: { key: remoteId } };
      default:
        return { status: 202, headers: { "content-type": "application/json" }, body: { id: remoteId, status: "accepted" } };
    }
  }
}

const RETRYABLE_SLACK_ERRORS = new Set(["ratelimited", "service_unavailable", "internal_error", "fatal_error"]);

const PRIORITY_MAP: Record<string, string> = { low: "Low", normal: "Medium", high: "High", urgent: "Highest" };

/** The one interface every integration implements. */
export abstract class Adapter {
  abstract readonly type: ConnectorSpec["type"];

  constructor(readonly spec: ConnectorSpec, readonly target: FakeTarget) {}

  /** Deliver task; remoteId is the id recorded for its previous revision. */
  abstract deliver(task: Task, idempotencyKey: string, remoteId?: string | null, replayed?: boolean): Promise<DeliveryResult>;

  /** Render the request deliver() will send, without sending it. */
  abstract render(task: Task, idempotencyKey: string, remoteId?: string | null): Promise<RenderedRequest>;

  secret(logical: string): string {
    const env = this.spec.secrets[logical];
    if (!env) throw new PermanentError(`connector ${this.spec.name} declares no secret ${logical}`);
    // The browser has no environment; the synthetic value stands in for os.environ[env].
    return `<${env}>`;
  }

  /** Resolve a mapping value: a bare field path or a $-template string. */
  renderField(template: string, task: Task): unknown {
    if (!template.includes("$")) return taskField(task, template);
    const values: Record<string, string> = {
      id: task.id,
      version: String(task.version),
      title: task.title,
      body: task.body,
      status: task.status,
      priority: task.priority,
      assignee: task.assignee ?? "",
      url: task.url ?? "",
      labels: task.labels.join(","),
    };
    for (const [k, v] of Object.entries(task.fields)) if (["string", "number"].includes(typeof v)) values[k] = String(v);
    return template.replace(/\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))/g, (m, a, b) => {
      const name = a ?? b;
      return name in values ? values[name] : m;
    });
  }

  mapped(task: Task): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [remote, source] of Object.entries(this.spec.mapping)) out[remote] = this.renderField(source, task);
    return out;
  }

  protected raiseForStatus(response: FakeResponse): void {
    const err = classifyResponse(response, this.spec.retry);
    if (err) throw err;
  }

  protected result(task: Task, key: string, remoteId: string | null): DeliveryResult {
    return { status: "delivered", connector: this.spec.name, taskId: task.id, idempotencyKey: key, remoteId, attempts: 1, detail: "" };
  }
}

export class SlackAdapter extends Adapter {
  readonly type = "slack" as const;

  get apiBase(): string {
    return (this.spec.baseUrl ?? "https://slack.com").replace(/\/$/, "");
  }

  get usesIncomingWebhook(): boolean {
    return this.spec.target.startsWith("https://");
  }

  blocks(task: Task): Record<string, unknown>[] {
    const mapped = this.mapped(task);
    const title = String(mapped.title ?? task.title);
    const body = String(mapped.body ?? task.body);
    const meta = [`*Status:* ${task.status}`, `*Priority:* ${task.priority}`];
    if (task.assignee) meta.push(`*Assignee:* ${task.assignee}`);
    const blocks: Record<string, unknown>[] = [
      { type: "header", text: { type: "plain_text", text: title.slice(0, 150) } },
      { type: "section", text: { type: "mrkdwn", text: (body || " ").slice(0, 3000) } },
      { type: "context", elements: [{ type: "mrkdwn", text: meta.join("  ") }] },
    ];
    if (task.url) blocks.push({ type: "actions", elements: [{ type: "button", text: { type: "plain_text", text: "Open task" }, url: task.url }] });
    return blocks;
  }

  async render(task: Task, key: string): Promise<RenderedRequest> {
    const body: Record<string, unknown> = {
      text: `${task.title} [${task.id} v${task.version}]`,
      blocks: this.blocks(task),
      metadata: { event_type: "conduit_task", event_payload: { task_id: task.id, version: task.version, idempotency_key: key } },
    };
    const headers: Record<string, string> = { "Idempotency-Key": key };
    let url = this.spec.target;
    if (!this.usesIncomingWebhook) {
      body.channel = this.spec.target;
      url = `${this.apiBase}/api/chat.postMessage`;
      headers.Authorization = `Bearer ${this.secret("token")}`;
    }
    return { method: "POST", url, headers, body };
  }

  async deliver(task: Task, key: string, _remoteId?: string | null, replayed = false): Promise<DeliveryResult> {
    const request = await this.render(task, key);
    const response = this.target.handle(request, task, replayed);
    this.raiseForStatus(response);
    // Slack returns HTTP 200 with ok: false for most API errors.
    if (this.usesIncomingWebhook) return this.result(task, key, null);
    if (response.body.ok) return this.result(task, key, String(response.body.ts ?? ""));
    const error = String(response.body.error ?? "unknown_error");
    if (RETRYABLE_SLACK_ERRORS.has(error)) throw new TransientError(`slack: ${error}`, response.status);
    throw new PermanentError(`slack: ${error}`, response.status);
  }
}

export const summaryTag = (taskId: string) => `[conduit:${taskId}]`;

export function adfParagraph(text: string): Record<string, unknown> {
  return { type: "doc", version: 1, content: [{ type: "paragraph", content: [{ type: "text", text: text || " " }] }] };
}

export class JiraAdapter extends Adapter {
  readonly type = "jira" as const;

  get apiBase(): string {
    return `${(this.spec.baseUrl ?? "").replace(/\/$/, "")}/rest/api/3`;
  }

  issueFields(task: Task): Record<string, unknown> {
    const mapped = this.mapped(task);
    let summary = String(mapped.summary ?? task.title);
    delete mapped.summary;
    const tag = summaryTag(task.id);
    if (!summary.includes(tag)) summary = `${summary} ${tag}`;
    const issuetype = String(mapped.issuetype ?? "Task");
    delete mapped.issuetype;
    const description = adfParagraph(String(mapped.description ?? task.body));
    delete mapped.description;
    const fields: Record<string, unknown> = {
      project: { key: this.spec.target },
      summary: summary.slice(0, 255),
      issuetype: { name: issuetype },
      description,
      labels: [...task.labels.map((l) => l.replace(/ /g, "-")), "conduit"],
    };
    const priority = mapped.priority ?? PRIORITY_MAP[task.priority];
    delete mapped.priority;
    if (priority) fields.priority = { name: String(priority) };
    for (const [remote, value] of Object.entries(mapped)) if (value !== null && value !== undefined) fields[remote] = value;
    return fields;
  }

  async render(task: Task, key: string, remoteId?: string | null): Promise<RenderedRequest> {
    const fields = this.issueFields(task);
    const headers = {
      "Idempotency-Key": key,
      Accept: "application/json",
      Authorization: `Basic base64(${this.secret("email")}:${this.secret("api_token")})`,
    };
    if (remoteId) {
      const { project: _p, ...rest } = fields;
      return { method: "PUT", url: `${this.apiBase}/issue/${remoteId}`, headers, body: { fields: rest } };
    }
    return { method: "POST", url: `${this.apiBase}/issue`, headers, body: { fields } };
  }

  async deliver(task: Task, key: string, remoteId?: string | null, replayed = false): Promise<DeliveryResult> {
    const request = await this.render(task, key, remoteId);
    const response = this.target.handle(request, task, replayed);
    if (remoteId && response.status === 404) {
      // a 404 on update falls back to create
      return this.deliver(task, key, null, replayed);
    }
    this.raiseForStatus(response);
    const issueKey = remoteId ?? (response.body.key ? String(response.body.key) : null);
    if (!issueKey) throw new PermanentError("jira: create response missing issue key");
    return this.result(task, key, issueKey);
  }
}

export const SIGNATURE_HEADER = "X-Conduit-Signature";
export const TIMESTAMP_HEADER = "X-Conduit-Timestamp";

const encoder = new TextEncoder();

export async function sign(secret: string, timestamp: string, body: string): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(`${timestamp}.${body}`));
  return `sha256=${Array.from(new Uint8Array(mac), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

/** json.dumps(..., separators=(",", ":"), sort_keys=True) */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const keys = Object.keys(value as Record<string, unknown>).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson((value as Record<string, unknown>)[k])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export class WebhookAdapter extends Adapter {
  readonly type = "webhook" as const;

  constructor(spec: ConnectorSpec, target: FakeTarget, private readonly clock: () => number) {
    super(spec, target);
  }

  payload(task: Task, key: string): Record<string, unknown> {
    const data = Object.keys(this.spec.mapping).length ? this.mapped(task) : { ...task };
    return { event: "task.synced", connector: this.spec.name, idempotency_key: key, task: data };
  }

  async render(task: Task, key: string): Promise<RenderedRequest> {
    const body = this.payload(task, key);
    const canonical = canonicalJson(body);
    const timestamp = String(Math.floor(1_700_000_000 + this.clock()));
    const headers = {
      "Content-Type": "application/json",
      "Idempotency-Key": key,
      [TIMESTAMP_HEADER]: timestamp,
      [SIGNATURE_HEADER]: await sign(this.secret("signing_secret"), timestamp, canonical),
    };
    return { method: "POST", url: this.spec.target, headers, body };
  }

  async deliver(task: Task, key: string, _remoteId?: string | null, replayed = false): Promise<DeliveryResult> {
    const request = await this.render(task, key);
    const response = this.target.handle(request, task, replayed);
    this.raiseForStatus(response);
    const id = response.headers["content-type"]?.startsWith("application/json") ? String(response.body.id ?? "") : "";
    return this.result(task, key, id || null);
  }
}

export function buildAdapter(spec: ConnectorSpec, target: FakeTarget, clock: () => number): Adapter {
  switch (spec.type) {
    case "slack":
      return new SlackAdapter(spec, target);
    case "jira":
      return new JiraAdapter(spec, target);
    case "webhook":
      return new WebhookAdapter(spec, target, clock);
  }
}
