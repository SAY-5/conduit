# Every *.yaml in connectors_dir becomes one connector. Adding an integration
# is: add a file, run terraform apply.
locals {
  connector_files = fileset(var.connectors_dir, "*.yaml")

  connectors = {
    for file in local.connector_files :
    trimsuffix(file, ".yaml") => merge(
      { name = trimsuffix(file, ".yaml") },
      yamldecode(file("${var.connectors_dir}/${file}"))
    )
  }
}

module "idempotency_table" {
  source = "./modules/idempotency-table"

  name = "${var.name_prefix}-idempotency"
}

module "connector" {
  source   = "./modules/connector"
  for_each = local.connectors

  name                   = each.key
  spec                   = each.value
  name_prefix            = var.name_prefix
  idempotency_table_arn  = module.idempotency_table.arn
  idempotency_table_name = module.idempotency_table.name
  container_image        = var.container_image
  enable_ecs             = var.enable_ecs
  ecs_execution_role_arn = var.ecs_execution_role_arn
  aws_region             = var.aws_region
}
