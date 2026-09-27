// Port of conduit/core/schema.py: the source contract a payload must meet before delivery.
// A payload that fails it is moved to the connector's quarantine queue, never the DLQ.
// The shipped schemas are parsed from schemas/<connector>/v<N>.yaml, embedded byte for
// byte in config.generated.ts.

import { taskField, type QuarantineNote, type Task } from "./models";
import { ConfigError, FIELD_TYPES, parseYaml, SCHEMA_YAML, type FieldType, type YamlScalar } from "./specs";

export interface SchemaField {
  type: FieldType;
  required?: boolean;
  enum?: YamlScalar[];
  maxLength?: number;
}

export interface SourceSchema {
  connector: string;
  version: number;
  fields: Record<string, SchemaField>;
}

/** Parse one schemas/<connector>/v<N>.yaml. */
export function loadSchema(connector: string, version: number, text: string): SourceSchema {
  const raw = parseYaml(text);
  const fields = raw.fields;
  if (!fields || typeof fields !== "object" || Array.isArray(fields)) {
    throw new ConfigError(`${connector}/v${version}: fields must be a mapping`);
  }
  const out: Record<string, SchemaField> = {};
  for (const [path, rule] of Object.entries(fields)) {
    if (!rule || typeof rule !== "object" || Array.isArray(rule)) throw new ConfigError(`${connector}/v${version}: ${path} must be a rule`);
    const type = rule.type;
    if (typeof type !== "string" || !FIELD_TYPES.includes(type as FieldType)) {
      throw new ConfigError(`${connector}/v${version}: ${path}.type must be one of ${FIELD_TYPES.join(", ")}`);
    }
    const field: SchemaField = { type: type as FieldType };
    if (rule.required !== undefined) field.required = rule.required === true;
    if (Array.isArray(rule.enum) && rule.enum.length) field.enum = rule.enum;
    if (typeof rule.max_length === "number") field.maxLength = rule.max_length;
    out[path] = field;
  }
  return { connector, version, fields: out };
}

/** The latest shipped version per connector, as in schemas/<connector>/v<N>.yaml. */
export const SCHEMAS: Record<string, SourceSchema> = Object.fromEntries(
  Object.entries(SCHEMA_YAML).map(([name, versions]) => {
    const latest = Math.max(...Object.keys(versions).map(Number));
    return [name, loadSchema(name, latest, versions[latest])];
  }),
);

function matches(value: unknown, type: FieldType): boolean {
  if (type === "any") return true;
  if (type === "boolean") return typeof value === "boolean";
  if (type === "string") return typeof value === "string";
  if (type === "integer") return typeof value === "number" && Number.isInteger(value);
  if (type === "number") return typeof value === "number";
  return Array.isArray(value);
}

function violation(rule: SchemaField, value: unknown): { reason: string; detail: string } | null {
  if (value === null) return rule.required ? { reason: "missing", detail: "required source field is absent" } : null;
  if (!matches(value, rule.type)) return { reason: "type", detail: `expected ${rule.type}, got ${Array.isArray(value) ? "list" : typeof value}` };
  if (rule.enum && !rule.enum.includes(value as YamlScalar)) return { reason: "enum", detail: `${JSON.stringify(value)} is not one of ${JSON.stringify(rule.enum)}` };
  const sized = typeof value === "string" || Array.isArray(value);
  if (rule.maxLength !== undefined && sized && value.length > rule.maxLength) return { reason: "too_long", detail: `length ${value.length} exceeds ${rule.maxLength}` };
  return null;
}

/** Check every field in order; the first violation wins, as in validate_payload. */
export function validatePayload(schema: SourceSchema, task: Task): QuarantineNote | null {
  for (const [path, rule] of Object.entries(schema.fields)) {
    const found = violation(rule, taskField(task, path));
    if (found) return { stage: "schema", field: path, reason: found.reason, detail: `${path}: ${found.detail}` };
  }
  return null;
}
