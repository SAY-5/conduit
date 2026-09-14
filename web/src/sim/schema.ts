// Port of conduit/core/schema.py: the source contract a payload must meet before delivery.
// A payload that fails it is moved to the connector's quarantine queue, never the DLQ.

import { taskField, type QuarantineNote, type Task } from "./models";

export type FieldType = "string" | "integer" | "number" | "boolean" | "list" | "any";

export interface SchemaField {
  type: FieldType;
  required?: boolean;
  enum?: unknown[];
  maxLength?: number;
}

export interface SourceSchema {
  connector: string;
  version: number;
  fields: Record<string, SchemaField>;
}

const ID: SchemaField = { type: "string", required: true, maxLength: 256 };
const TITLE: SchemaField = { type: "string", required: true };

/** The latest shipped version per connector, as in schemas/<connector>/v<N>.yaml. */
export const SCHEMAS: Record<string, SourceSchema> = {
  "jira-support": {
    connector: "jira-support",
    version: 2,
    fields: { id: ID, title: TITLE, priority: { type: "string", enum: ["low", "normal", "high", "urgent"] }, "fields.region": { type: "string" }, "fields.reporter": { type: "string" } },
  },
  "slack-ops": {
    connector: "slack-ops",
    version: 1,
    fields: { id: ID, title: TITLE, body: { type: "string" }, labels: { type: "list" } },
  },
  "webhook-crm": {
    connector: "webhook-crm",
    version: 1,
    fields: { id: ID, title: TITLE, version: { type: "integer", required: true }, status: { type: "string", enum: ["open", "in_progress", "blocked", "done", "closed"] } },
  },
};

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
  if (rule.enum && !rule.enum.includes(value)) return { reason: "enum", detail: `${JSON.stringify(value)} is not one of ${JSON.stringify(rule.enum)}` };
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
