# Everything one integration needs, derived from its YAML: queue + DLQ,
# a least-privilege policy and role for its worker, an SSM placeholder per
# secret, and a container definition (optionally registered on ECS).
locals {
  queue_spec  = try(var.spec.queue, {})
  secrets     = try(var.spec.secrets, {})
  full_name   = "${var.name_prefix}-${var.name}"
  ssm_prefix  = "/${var.name_prefix}/${var.name}"
  metric_port = 9100

  container_definition = {
    name      = local.full_name
    image     = var.container_image
    essential = true
    command   = ["conduit", "worker", "--connector", var.name, "--json-logs"]
    environment = [
      { name = "AWS_DEFAULT_REGION", value = var.aws_region },
      { name = "CONDUIT_CONNECTORS_DIR", value = "/app/connectors" },
      { name = "CONDUIT_TABLE", value = var.idempotency_table_name },
      { name = "CONDUIT_METRICS_PORT", value = tostring(local.metric_port) },
    ]
    secrets = [
      for logical, env_name in local.secrets : {
        name      = env_name
        valueFrom = aws_ssm_parameter.secret[logical].arn
      }
    ]
    portMappings = [{ containerPort = local.metric_port, protocol = "tcp" }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = "/conduit/${var.name}"
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "worker"
      }
    }
  }
}

module "queue" {
  source = "../queue"

  name                       = local.full_name
  max_receive_count          = try(local.queue_spec.max_receive_count, 3)
  visibility_timeout_seconds = try(local.queue_spec.visibility_timeout_seconds, 60)
  message_retention_seconds  = try(local.queue_spec.message_retention_seconds, 345600)
  dlq_retention_seconds      = try(local.queue_spec.dlq_retention_seconds, 1209600)
  tags                       = { Connector = var.name, ConnectorType = var.spec.type }
}

resource "aws_ssm_parameter" "secret" {
  for_each = local.secrets

  name        = "${local.ssm_prefix}/${each.value}"
  description = "Secret '${each.key}' for connector ${var.name}; set the real value out of band."
  type        = "SecureString"
  value       = "CHANGE_ME"
  tags        = { Connector = var.name }

  lifecycle {
    ignore_changes = [value]
  }
}

data "aws_iam_policy_document" "worker" {
  statement {
    sid = "Queue"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:DeleteMessageBatch",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
    ]
    resources = [module.queue.queue_arn, module.queue.dlq_arn]
  }

  statement {
    sid = "Idempotency"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
    ]
    resources = [var.idempotency_table_arn]
  }

  dynamic "statement" {
    for_each = length(local.secrets) > 0 ? [1] : []
    content {
      sid       = "Secrets"
      actions   = ["ssm:GetParameter", "ssm:GetParameters"]
      resources = [for p in aws_ssm_parameter.secret : p.arn]
    }
  }
}

resource "aws_iam_policy" "worker" {
  name   = "${local.full_name}-worker"
  policy = data.aws_iam_policy_document.worker.json
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker" {
  name               = "${local.full_name}-worker"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy_attachment" "worker" {
  role       = aws_iam_role.worker.name
  policy_arn = aws_iam_policy.worker.arn
}

resource "aws_ecs_task_definition" "worker" {
  count = var.enable_ecs ? 1 : 0

  family                   = local.full_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  task_role_arn            = aws_iam_role.worker.arn
  execution_role_arn       = var.ecs_execution_role_arn
  container_definitions    = jsonencode([local.container_definition])
}
