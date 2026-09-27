// Port of conduit/core/mapping.py: render, default, coerce, validate, and truncate
// every remote field before delivery. The worker runs validateTask before it claims
// a key, so a task that can never be delivered goes to quarantine up front.

import { taskField, type Task } from "./models";
import type { ConnectorSpec, FieldRule, FieldType, YamlScalar } from "./specs";

const TRUE_WORDS = new Set(["true", "1", "yes", "on"]);
const FALSE_WORDS = new Set(["false", "0", "no", "off"]);

/** A task does not fit the connector's mapping; `reason` is the metric label. */
export class MappingError extends Error {
  constructor(
    readonly field: string,
    readonly reason: string,
    readonly detail: string,
  ) {
    super(`${field}: ${detail}`);
    this.name = "MappingError";
  }
}

/** Resolve a bare field path (type preserved) or a $-template (string). */
export function render(source: string, task: Task): unknown {
  if (!source.includes("$")) return taskField(task, source);
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
  for (const [k, v] of Object.entries(task.fields)) if (typeof v === "string" || typeof v === "number") values[k] = String(v);
  return source.replace(/\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))/g, (m, a, b) => {
    const name = a ?? b;
    return name in values ? values[name] : m;
  });
}

/** Convert value to type; throws when it cannot be read that way. */
export function coerce(value: unknown, type: FieldType): unknown {
  if (type === "any") return value;
  if (type === "string") return typeof value === "string" ? value : String(value);
  if (type === "integer") {
    if (typeof value === "boolean") throw new Error("boolean is not an integer");
    const n = typeof value === "number" ? value : /^-?\d+$/.test(String(value).trim()) ? Number(value) : NaN;
    if (!Number.isInteger(n)) throw new Error("not a whole number");
    return n;
  }
  if (type === "number") {
    if (typeof value === "boolean") throw new Error("boolean is not a number");
    const n = Number(value);
    if (typeof value !== "number" && (String(value).trim() === "" || !Number.isFinite(n))) throw new Error("not a number");
    return n;
  }
  if (type === "boolean") {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value !== 0;
    const word = String(value).trim().toLowerCase();
    if (TRUE_WORDS.has(word)) return true;
    if (FALSE_WORDS.has(word)) return false;
    throw new Error("not a boolean word");
  }
  if (Array.isArray(value)) return [...value];
  if (typeof value === "string") return value.split(",").map((p) => p.trim()).filter(Boolean);
  throw new Error("not a list");
}

export function applyRule(remote: string, rule: FieldRule, task: Task): unknown {
  let value: unknown = rule.source !== null ? render(rule.source, task) : null;
  if (value === null || value === undefined) value = rule.default;
  if (value === null || value === undefined) {
    if (rule.required) throw new MappingError(remote, "required", "required field is missing");
    return null;
  }
  try {
    value = coerce(value, rule.type);
  } catch (exc) {
    const why = exc instanceof Error ? exc.message : String(exc);
    throw new MappingError(remote, "type", `expected ${rule.type}, got ${JSON.stringify(value)} (${why})`);
  }
  if (rule.enum !== null && !rule.enum.includes(value as YamlScalar)) {
    throw new MappingError(remote, "enum", `${JSON.stringify(value)} is not one of ${JSON.stringify(rule.enum)}`);
  }
  if (rule.maxLength !== null && (typeof value === "string" || Array.isArray(value)) && value.length > rule.maxLength) {
    if (!rule.truncate) throw new MappingError(remote, "max_length", `length ${value.length} exceeds ${rule.maxLength}`);
    value = value.slice(0, rule.maxLength);
  }
  return value;
}

/** Every remote field for task; throws MappingError on the first violation. */
export function applyMapping(spec: ConnectorSpec, task: Task): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [remote, rule] of Object.entries(spec.mapping)) out[remote] = applyRule(remote, rule, task);
  return out;
}

export function validateTask(spec: ConnectorSpec, task: Task): MappingError | null {
  try {
    applyMapping(spec, task);
  } catch (err) {
    if (err instanceof MappingError) return err;
    throw err;
  }
  return null;
}
