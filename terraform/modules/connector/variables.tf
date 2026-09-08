variable "name" {
  type = string
}

variable "spec" {
  description = "Decoded connector YAML."
  type        = any
}

variable "name_prefix" {
  type = string
}

variable "idempotency_table_arn" {
  type = string
}

variable "idempotency_table_name" {
  type = string
}

variable "container_image" {
  type = string
}

variable "enable_ecs" {
  type    = bool
  default = false
}

variable "ecs_execution_role_arn" {
  type    = string
  default = null
}

variable "aws_region" {
  type = string
}
