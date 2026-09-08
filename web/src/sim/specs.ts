// Port of conduit/config.py: connector specs, one YAML per integration.
// The three shipped YAMLs are embedded verbatim and parsed by the same
// small parser the editor uses for a fourth file.

export type ConnectorType = "slack" | "jira" | "webhook";

export interface RetryPolicy {
  maxAttempts: number;
  baseSeconds: number;
  maxSeconds: number;
  multiplier: number;
  retryOnStatus: number[];
  timeoutSeconds: number;
}

export interface QueueSpec {
  maxReceiveCount: number;
  visibilityTimeoutSeconds: number;
  messageRetentionSeconds: number;
  dlqRetentionSeconds: number;
}

export interface ConnectorSpec {
  name: string;
  type: ConnectorType;
  target: string;
  baseUrl: string | null;
  secrets: Record<string, string>;
  mapping: Record<string, string>;
  retry: RetryPolicy;
  rateLimit: { requestsPerSecond: number };
  queue: QueueSpec;
  idempotencyTtlSeconds: number;
}

export const DEFAULT_RETRY: RetryPolicy = {
  maxAttempts: 5,
  baseSeconds: 0.5,
  maxSeconds: 30,
  multiplier: 2,
  retryOnStatus: [408, 425, 429, 500, 502, 503, 504],
  timeoutSeconds: 10,
};

export const DEFAULT_QUEUE: QueueSpec = {
  maxReceiveCount: 3,
  visibilityTimeoutSeconds: 60,
  messageRetentionSeconds: 345600,
  dlqRetentionSeconds: 1209600,
};

export const REQUIRED_SECRETS: Record<ConnectorType, string[]> = {
  slack: ["token"],
  jira: ["email", "api_token"],
  webhook: ["signing_secret"],
};

export const queueName = (spec: ConnectorSpec) => `conduit-${spec.name}`;
export const dlqName = (spec: ConnectorSpec) => `conduit-${spec.name}-dlq`;

export const CONNECTOR_YAML: Record<string, string> = {
  "jira-support": `# Create or update an issue in the SUP project for every task revision.
type: jira
target: SUP
base_url: \${JIRA_BASE_URL:-https://example.atlassian.net}
secrets:
  email: JIRA_EMAIL
  api_token: JIRA_API_TOKEN
mapping:
  summary: title
  description: body
  issuetype: "Task"
  customfield_10042: id            # external task id field
retry:
  max_attempts: 4
  base_seconds: 0.25
  max_seconds: 8
rate_limit:
  requests_per_second: 10
queue:
  max_receive_count: 3
  visibility_timeout_seconds: 45
idempotency_ttl_seconds: 1209600
`,
  "slack-ops": `# Post every task to the #ops channel as a Block Kit message.
type: slack
target: "#ops"
base_url: \${SLACK_BASE_URL:-https://slack.com}   # the demo points this at a fake
secrets:
  token: SLACK_BOT_TOKEN
mapping:
  title: "[$priority] $title"
  body: body
retry:
  max_attempts: 5
  base_seconds: 0.2
  max_seconds: 5
rate_limit:
  requests_per_second: 20
queue:
  max_receive_count: 3
  visibility_timeout_seconds: 30
`,
  "webhook-crm": `# Signed JSON POST to the CRM ingest endpoint.
type: webhook
target: \${CRM_WEBHOOK_URL:-https://crm.example.com/hooks/conduit}
secrets:
  signing_secret: CRM_WEBHOOK_SECRET
mapping:
  external_id: id
  revision: version
  name: title
  notes: body
  state: status
  owner: assignee
retry:
  max_attempts: 3
  base_seconds: 0.1
  max_seconds: 2
rate_limit:
  requests_per_second: 50
queue:
  max_receive_count: 2
  visibility_timeout_seconds: 20
`,
};

export const NEW_CONNECTOR_YAML = `# connectors/pager-oncall.yaml
type: slack
target: "#oncall"
secrets:
  token: PAGER_SLACK_TOKEN
retry:
  max_attempts: 6
queue:
  max_receive_count: 4
`;

// A small YAML subset: nested maps by two-space indentation, scalars, comments.
export type YamlValue = string | number | boolean | null | YamlMap;
export interface YamlMap {
  [key: string]: YamlValue;
}

export class ConfigError extends Error {}

export function parseYaml(text: string): YamlMap {
  const root: YamlMap = {};
  const stack: { indent: number; map: YamlMap }[] = [{ indent: -1, map: root }];
  const lines = text.split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const stripped = stripComment(raw);
    if (!stripped.trim()) continue;
    const indent = stripped.length - stripped.trimStart().length;
    const body = stripped.trim();
    const colon = body.indexOf(":");
    if (colon <= 0) throw new ConfigError(`line ${i + 1}: expected "key: value"`);
    const key = body.slice(0, colon).trim();
    const rest = body.slice(colon + 1).trim();
    while (stack.length > 1 && indent <= stack[stack.length - 1].indent) stack.pop();
    const parent = stack[stack.length - 1].map;
    if (rest === "") {
      const child: YamlMap = {};
      parent[key] = child;
      stack.push({ indent, map: child });
    } else {
      parent[key] = parseScalar(rest);
    }
  }
  return root;
}

function stripComment(line: string): string {
  let inQuote: string | null = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuote) {
      if (ch === inQuote) inQuote = null;
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === "#" && (i === 0 || /\s/.test(line[i - 1]))) {
      return line.slice(0, i);
    }
  }
  return line;
}

function parseScalar(s: string): YamlValue {
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    return s.slice(1, -1);
  }
  if (s === "true") return true;
  if (s === "false") return false;
  if (s === "null" || s === "~") return null;
  if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);
  return s;
}

const ENV_PATTERN = /\$\{([A-Z][A-Z0-9_]*)(?::-([^}]*))?\}/g;

/** Expand ${VAR} and ${VAR:-default}; the browser has no environment, so defaults win. */
export function interpolate(value: YamlValue, env: Record<string, string> = {}): YamlValue {
  if (typeof value === "string") {
    return value.replace(ENV_PATTERN, (_, name: string, def: string | undefined) => {
      if (name in env) return env[name];
      if (def !== undefined) return def;
      throw new ConfigError(`environment variable ${name} is referenced but not set`);
    });
  }
  if (value && typeof value === "object") {
    const out: YamlMap = {};
    for (const [k, v] of Object.entries(value)) out[k] = interpolate(v, env);
    return out;
  }
  return value;
}

function num(map: YamlMap, key: string, fallback: number, lo: number, hi: number): number {
  const v = map[key];
  if (v === undefined) return fallback;
  if (typeof v !== "number") throw new ConfigError(`${key} must be a number`);
  if (v < lo || v > hi) throw new ConfigError(`${key} must be between ${lo} and ${hi}`);
  return v;
}

function strMap(map: YamlMap, key: string): Record<string, string> {
  const v = map[key];
  if (v === undefined || v === null) return {};
  if (typeof v !== "object") throw new ConfigError(`${key} must be a mapping`);
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v)) out[k] = String(val);
  return out;
}

export function loadSpec(name: string, text: string, env: Record<string, string> = {}): ConnectorSpec {
  if (!/^[a-z0-9][a-z0-9-]{0,62}$/.test(name)) {
    throw new ConfigError(`connector name ${JSON.stringify(name)} must match ^[a-z0-9][a-z0-9-]{0,62}$`);
  }
  const raw = interpolate(parseYaml(text), env) as YamlMap;
  const type = raw.type;
  if (type !== "slack" && type !== "jira" && type !== "webhook") {
    throw new ConfigError(`type must be one of slack, jira, webhook (got ${JSON.stringify(type ?? null)})`);
  }
  const target = raw.target;
  if (typeof target !== "string" || !target) throw new ConfigError("target is required");
  const secrets = strMap(raw, "secrets");
  for (const [logical, envName] of Object.entries(secrets)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(envName) || envName !== envName.toUpperCase()) {
      throw new ConfigError(`secret ${JSON.stringify(logical)} must map to an UPPER_CASE env var name`);
    }
  }
  const missing = REQUIRED_SECRETS[type].filter((s) => !(s in secrets));
  if (missing.length) throw new ConfigError(`${type} connector requires secrets: [${missing.map((m) => `'${m}'`).join(", ")}]`);
  const baseUrl = typeof raw.base_url === "string" ? raw.base_url : null;
  if (type === "jira" && !baseUrl) throw new ConfigError("jira connector requires base_url");
  const retryRaw = (raw.retry as YamlMap) ?? {};
  const retry: RetryPolicy = {
    maxAttempts: num(retryRaw, "max_attempts", DEFAULT_RETRY.maxAttempts, 1, 20),
    baseSeconds: num(retryRaw, "base_seconds", DEFAULT_RETRY.baseSeconds, 1e-9, 1e9),
    maxSeconds: num(retryRaw, "max_seconds", DEFAULT_RETRY.maxSeconds, 1e-9, 1e9),
    multiplier: num(retryRaw, "multiplier", DEFAULT_RETRY.multiplier, 1, 1e9),
    retryOnStatus: DEFAULT_RETRY.retryOnStatus,
    timeoutSeconds: num(retryRaw, "timeout_seconds", DEFAULT_RETRY.timeoutSeconds, 1e-9, 1e9),
  };
  if (retry.maxSeconds < retry.baseSeconds) throw new ConfigError("max_seconds must be >= base_seconds");
  const queueRaw = (raw.queue as YamlMap) ?? {};
  const queue: QueueSpec = {
    maxReceiveCount: num(queueRaw, "max_receive_count", DEFAULT_QUEUE.maxReceiveCount, 1, 100),
    visibilityTimeoutSeconds: num(queueRaw, "visibility_timeout_seconds", DEFAULT_QUEUE.visibilityTimeoutSeconds, 1, 43200),
    messageRetentionSeconds: num(queueRaw, "message_retention_seconds", DEFAULT_QUEUE.messageRetentionSeconds, 60, 1209600),
    dlqRetentionSeconds: num(queueRaw, "dlq_retention_seconds", DEFAULT_QUEUE.dlqRetentionSeconds, 60, 1209600),
  };
  const rateRaw = (raw.rate_limit as YamlMap) ?? {};
  return {
    name,
    type,
    target,
    baseUrl,
    secrets,
    mapping: strMap(raw, "mapping"),
    retry,
    rateLimit: { requestsPerSecond: num(rateRaw, "requests_per_second", 10, 1e-9, 1e9) },
    queue,
    idempotencyTtlSeconds: num(raw, "idempotency_ttl_seconds", 7 * 24 * 3600, 60, 1e12),
  };
}

export function loadAll(files: Record<string, string> = CONNECTOR_YAML): Record<string, ConnectorSpec> {
  const specs: Record<string, ConnectorSpec> = {};
  for (const name of Object.keys(files).sort()) specs[name] = loadSpec(name, files[name]);
  return specs;
}

export const CONNECTOR_NAMES = Object.keys(CONNECTOR_YAML).sort();
