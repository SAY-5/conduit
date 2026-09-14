// Model of terraform/: fileset(connectors_dir, "*.yaml") -> yamldecode -> module.connector for_each.
// Derives the same resource set the real modules create, and diffs two plans.

import { dlqName, loadSpec, quarantineName, queueName, type ConnectorSpec } from "./specs";

export interface PlannedResource {
  address: string;
  type: string;
  module: string;
  attributes: Record<string, string>;
}

const NAME_PREFIX = "conduit";

export function connectorResources(spec: ConnectorSpec): PlannedResource[] {
  const mod = `module.connector["${spec.name}"]`;
  const full = `${NAME_PREFIX}-${spec.name}`;
  const q = queueName(spec);
  const dlq = dlqName(spec);
  const quarantine = quarantineName(spec);
  const out: PlannedResource[] = [
    {
      address: `${mod}.aws_iam_policy.worker`,
      type: "aws_iam_policy",
      module: mod,
      attributes: {
        name: `${full}-worker`,
        policy: `sqs:* on ${q}, ${dlq}, ${quarantine}; dynamodb:PutItem/GetItem/UpdateItem/DeleteItem on conduit-idempotency` + (Object.keys(spec.secrets).length ? `; ssm:GetParameter on ${Object.keys(spec.secrets).length} parameter(s)` : ""),
      },
    },
    {
      address: `${mod}.aws_iam_role.worker`,
      type: "aws_iam_role",
      module: mod,
      attributes: { name: `${full}-worker`, assume_role_policy: "sts:AssumeRole from ecs-tasks.amazonaws.com" },
    },
    {
      address: `${mod}.aws_iam_role_policy_attachment.worker`,
      type: "aws_iam_role_policy_attachment",
      module: mod,
      attributes: { role: `${full}-worker`, policy_arn: `(known after apply)` },
    },
  ];
  for (const [logical, env] of Object.entries(spec.secrets).sort()) {
    out.push({
      address: `${mod}.aws_ssm_parameter.secret["${logical}"]`,
      type: "aws_ssm_parameter",
      module: mod,
      attributes: { name: `/${NAME_PREFIX}/${spec.name}/${env}`, type: "SecureString", value: "(sensitive value)", description: `Secret '${logical}' for connector ${spec.name}; set the real value out of band.` },
    });
  }
  out.push(
    {
      address: `${mod}.module.queue.aws_sqs_queue.dlq`,
      type: "aws_sqs_queue",
      module: `${mod}.module.queue`,
      attributes: { name: dlq, message_retention_seconds: String(spec.queue.dlqRetentionSeconds), sqs_managed_sse_enabled: "true" },
    },
    {
      address: `${mod}.module.queue.aws_sqs_queue.main`,
      type: "aws_sqs_queue",
      module: `${mod}.module.queue`,
      attributes: {
        name: q,
        visibility_timeout_seconds: String(spec.queue.visibilityTimeoutSeconds),
        message_retention_seconds: String(spec.queue.messageRetentionSeconds),
        receive_wait_time_seconds: "20",
        redrive_policy: JSON.stringify({ deadLetterTargetArn: `arn:aws:sqs:us-east-1:000000000000:${dlq}`, maxReceiveCount: spec.queue.maxReceiveCount }),
      },
    },
    {
      address: `${mod}.module.queue.aws_sqs_queue.quarantine`,
      type: "aws_sqs_queue",
      module: `${mod}.module.queue`,
      attributes: { name: quarantine, message_retention_seconds: String(spec.queue.dlqRetentionSeconds), sqs_managed_sse_enabled: "true" },
    },
    {
      address: `${mod}.module.queue.aws_sqs_queue_redrive_allow_policy.dlq`,
      type: "aws_sqs_queue_redrive_allow_policy",
      module: `${mod}.module.queue`,
      attributes: { queue_url: `(known after apply)`, redrive_allow_policy: JSON.stringify({ redrivePermission: "byQueue", sourceQueueArns: [`arn:aws:sqs:us-east-1:000000000000:${q}`] }) },
    },
  );
  return out;
}

export const TABLE_RESOURCE: PlannedResource = {
  address: "module.idempotency_table.aws_dynamodb_table.this",
  type: "aws_dynamodb_table",
  module: "module.idempotency_table",
  attributes: { name: "conduit-idempotency", billing_mode: "PAY_PER_REQUEST", hash_key: "pk", ttl: "expires_at", point_in_time_recovery: "true" },
};

export interface Plan {
  resources: PlannedResource[];
  connectors: string[];
}

/** terraform plan over a connectors directory: one module instance per YAML plus the shared table. */
export function plan(files: Record<string, string>): Plan {
  const names = Object.keys(files).sort();
  const resources: PlannedResource[] = [TABLE_RESOURCE];
  for (const name of names) resources.push(...connectorResources(loadSpec(name, files[name])));
  return { resources, connectors: names };
}

export interface PlanDiff {
  add: PlannedResource[];
  change: PlannedResource[];
  destroy: PlannedResource[];
  summary: string;
}

export function diffPlans(before: Plan, after: Plan): PlanDiff {
  const b = new Map(before.resources.map((r) => [r.address, r]));
  const a = new Map(after.resources.map((r) => [r.address, r]));
  const add: PlannedResource[] = [];
  const change: PlannedResource[] = [];
  const destroy: PlannedResource[] = [];
  for (const [addr, r] of a) {
    const prev = b.get(addr);
    if (!prev) add.push(r);
    else if (JSON.stringify(prev.attributes) !== JSON.stringify(r.attributes)) change.push(r);
  }
  for (const [addr, r] of b) if (!a.has(addr)) destroy.push(r);
  return { add, change, destroy, summary: `Plan: ${add.length} to add, ${change.length} to change, ${destroy.length} to destroy.` };
}
