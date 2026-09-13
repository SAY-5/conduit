output "connectors" {
  description = "Queue, DLQ, quarantine queue, and policy per connector."
  value = {
    for name, mod in module.connector : name => {
      type              = mod.type
      queue_url         = mod.queue_url
      dlq_url           = mod.dlq_url
      quarantine_url    = mod.quarantine_url
      max_receive_count = mod.max_receive_count
      policy_arn        = mod.policy_arn
      role_arn          = mod.role_arn
      ssm_parameters    = mod.ssm_parameters
    }
  }
}

output "idempotency_table" {
  value = module.idempotency_table.name
}

output "container_definitions" {
  description = "Rendered container definition per connector for any container scheduler."
  value       = { for name, mod in module.connector : name => mod.container_definition }
}
