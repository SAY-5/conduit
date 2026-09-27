// Port of conduit/config.py: connector specs, one YAML per integration.
// The three shipped YAMLs come from config.generated.ts, which scripts/embed-config.mjs
// writes from connectors/*.yaml byte for byte; they are parsed by the same small
// parser the editor uses for a fourth file.

import { CONNECTOR_YAML } from "./config.generated";

export { CONNECTOR_YAML, SCHEMA_YAML, SOURCE_PY } from "./config.generated";

export type ConnectorType = "slack" | "jira" | "webhook";

export type FieldType = "string" | "integer" | "number" | "boolean" | "list" | "any";
export const FIELD_TYPES: FieldType[] = ["string", "integer", "number", "boolean", "list", "any"];

export type YamlScalar = string | number | boolean | null;

/**
 * One remote field: where its value comes from and what shape it must have, as
 * conduit/config.py FieldRule. A bare string in the YAML is a rule with only a
 * source; a rule with a default and no source is a constant.
 */
export interface FieldRule {
  source: string | null;
  type: FieldType;
  required: boolean;
  enum: YamlScalar[] | null;
  default: YamlScalar | null;
  maxLength: number | null;
  truncate: boolean;
}

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

/** Token bucket: requests_per_second refill, burst capacity, Retry-After cap. */
export interface RateLimit {
  requestsPerSecond: number;
  burst: number;
  maxRetryAfterSeconds: number;
}

/** Open after failureThreshold consecutive target failures; probe after recovery. */
export interface BreakerSpec {
  failureThreshold: number;
  recoverySeconds: number;
}

export interface ConnectorSpec {
  name: string;
  type: ConnectorType;
  target: string;
  baseUrl: string | null;
  secrets: Record<string, string>;
  mapping: Record<string, FieldRule>;
  retry: RetryPolicy;
  rateLimit: RateLimit;
  breaker: BreakerSpec;
  queue: QueueSpec;
  idempotencyTtlSeconds: number;
}

export const DEFAULT_RATE_LIMIT: RateLimit = {
  requestsPerSecond: 10,
  burst: 1,
  maxRetryAfterSeconds: 60,
};

export const DEFAULT_BREAKER: BreakerSpec = {
  failureThreshold: 5,
  recoverySeconds: 30,
};

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
export const quarantineName = (spec: ConnectorSpec) => `conduit-${spec.name}-quarantine`;

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

/**
 * How long the file is. The panel prints the YAML with a filename comment on top that the file
 * demo/run.py writes does not carry, so the comment is left out and both surfaces report the
 * same count for the same file.
 */
export const NEW_CONNECTOR_LINES = NEW_CONNECTOR_YAML.trimEnd()
  .split("\n")
  .filter((line) => !line.startsWith("#")).length;

// A small YAML subset: nested maps by two-space indentation, scalars, inline
// lists of scalars, comments.
export type YamlValue = YamlScalar | YamlScalar[] | YamlMap;
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
      parent[key] = parseValue(rest);
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

function parseValue(s: string): YamlScalar | YamlScalar[] {
  if (s.startsWith("[") && s.endsWith("]")) {
    const inner = s.slice(1, -1).trim();
    return inner === "" ? [] : inner.split(",").map((part) => parseScalar(part.trim()));
  }
  return parseScalar(s);
}

function parseScalar(s: string): YamlScalar {
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
  if (Array.isArray(value)) return value.map((v) => interpolate(v, env) as YamlScalar);
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
  if (typeof v !== "object" || Array.isArray(v)) throw new ConfigError(`${key} must be a mapping`);
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v)) out[k] = String(val);
  return out;
}

const isScalar = (v: unknown): v is YamlScalar => v === null || ["string", "number", "boolean"].includes(typeof v);

function fieldRule(remote: string, raw: YamlValue): FieldRule {
  const rule: FieldRule = { source: null, type: "any", required: false, enum: null, default: null, maxLength: null, truncate: true };
  if (typeof raw === "string") return { ...rule, source: raw };
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new ConfigError(`mapping.${remote} must be a source string or a rule`);
  }
  if (raw.source !== undefined && raw.source !== null) {
    if (typeof raw.source !== "string") throw new ConfigError(`mapping.${remote}.source must be a string`);
    rule.source = raw.source;
  }
  if (raw.type !== undefined) {
    if (typeof raw.type !== "string" || !FIELD_TYPES.includes(raw.type as FieldType)) {
      throw new ConfigError(`mapping.${remote}.type must be one of ${FIELD_TYPES.join(", ")}`);
    }
    rule.type = raw.type as FieldType;
  }
  if (raw.required !== undefined) {
    if (typeof raw.required !== "boolean") throw new ConfigError(`mapping.${remote}.required must be true or false`);
    rule.required = raw.required;
  }
  if (raw.enum !== undefined && raw.enum !== null) {
    if (!Array.isArray(raw.enum) || raw.enum.length === 0) throw new ConfigError(`mapping.${remote}.enum must list at least one value`);
    rule.enum = raw.enum;
  }
  if (raw.default !== undefined) {
    if (!isScalar(raw.default)) throw new ConfigError(`mapping.${remote}.default must be a scalar`);
    rule.default = raw.default;
  }
  if (raw.max_length !== undefined) rule.maxLength = num(raw, "max_length", 1, 1, 1e9);
  if (raw.truncate !== undefined) {
    if (typeof raw.truncate !== "boolean") throw new ConfigError(`mapping.${remote}.truncate must be true or false`);
    rule.truncate = raw.truncate;
  }
  if (rule.source === null && rule.default === null) throw new ConfigError(`mapping.${remote} needs a source or a default`);
  return rule;
}

function mappingRules(raw: YamlMap): Record<string, FieldRule> {
  const v = raw.mapping;
  if (v === undefined || v === null) return {};
  if (typeof v !== "object" || Array.isArray(v)) throw new ConfigError("mapping must be a mapping");
  const out: Record<string, FieldRule> = {};
  for (const [remote, rule] of Object.entries(v)) out[remote] = fieldRule(remote, rule);
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
  const breakerRaw = (raw.breaker as YamlMap) ?? {};
  return {
    name,
    type,
    target,
    baseUrl,
    secrets,
    mapping: mappingRules(raw),
    retry,
    rateLimit: {
      requestsPerSecond: num(rateRaw, "requests_per_second", DEFAULT_RATE_LIMIT.requestsPerSecond, 1e-9, 1e9),
      burst: num(rateRaw, "burst", DEFAULT_RATE_LIMIT.burst, 1, 1000),
      maxRetryAfterSeconds: num(rateRaw, "max_retry_after_seconds", DEFAULT_RATE_LIMIT.maxRetryAfterSeconds, 1e-9, 1e9),
    },
    breaker: {
      failureThreshold: num(breakerRaw, "failure_threshold", DEFAULT_BREAKER.failureThreshold, 1, 1000),
      recoverySeconds: num(breakerRaw, "recovery_seconds", DEFAULT_BREAKER.recoverySeconds, 1e-9, 1e9),
    },
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

/** One line per rule, the way `conduit config show` would describe it. */
export function describeRule(rule: FieldRule): string {
  const parts: string[] = [];
  if (rule.type !== "any") parts.push(rule.type);
  if (rule.required) parts.push("required");
  if (rule.enum) parts.push(`one of ${rule.enum.map(String).join("|")}`);
  if (rule.default !== null) parts.push(`default ${String(rule.default)}`);
  if (rule.maxLength !== null) parts.push(`max_length ${rule.maxLength}${rule.truncate ? "" : ", rejected when longer"}`);
  const head = rule.source ?? "(constant)";
  return parts.length ? `${head} (${parts.join(", ")})` : head;
}
