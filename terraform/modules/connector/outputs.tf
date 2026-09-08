output "type" {
  value = var.spec.type
}

output "queue_url" {
  value = module.queue.queue_url
}

output "dlq_url" {
  value = module.queue.dlq_url
}

output "max_receive_count" {
  value = try(local.queue_spec.max_receive_count, 3)
}

output "policy_arn" {
  value = aws_iam_policy.worker.arn
}

output "role_arn" {
  value = aws_iam_role.worker.arn
}

output "ssm_parameters" {
  value = { for logical, p in aws_ssm_parameter.secret : logical => p.name }
}

output "container_definition" {
  value = local.container_definition
}
