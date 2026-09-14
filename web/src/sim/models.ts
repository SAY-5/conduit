// Port of conduit/models.py: task payloads and delivery outcomes.

export interface Task {
  id: string;
  version: number;
  title: string;
  body: string;
  status: string;
  priority: string;
  assignee: string | null;
  labels: string[];
  url: string | null;
  fields: Record<string, unknown>;
}

export function makeTask(partial: Partial<Task> & { id: string; title: string }): Task {
  return {
    version: 1,
    body: "",
    status: "open",
    priority: "normal",
    assignee: null,
    labels: [],
    url: null,
    fields: {},
    ...partial,
  };
}

/** Resolve a dotted path against the task, used by field mappings. */
export function taskField(task: Task, path: string): unknown {
  let current: unknown = task;
  for (const part of path.split(".")) {
    if (current && typeof current === "object") {
      current = (current as Record<string, unknown>)[part];
    } else {
      return null;
    }
    if (current === undefined || current === null) return null;
  }
  return current;
}

export type DeliveryStatus = "delivered" | "deduplicated" | "failed" | "quarantined";

/** Why a message was set aside, carried with it into the quarantine queue. */
export interface QuarantineNote {
  stage: string;
  field: string;
  reason: string;
  detail: string;
}

export interface DeliveryResult {
  status: DeliveryStatus;
  connector: string;
  taskId: string;
  idempotencyKey: string;
  remoteId: string | null;
  attempts: number;
  detail: string;
}

/** The message body placed on the connector queue. */
export interface Envelope {
  task: Task;
  connector: string;
  idempotencyKey: string;
  submittedAt: number;
  attempt: number;
  quarantine?: QuarantineNote;
}
