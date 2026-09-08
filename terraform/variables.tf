variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for every resource name; matches the worker's queue naming."
  type        = string
  default     = "conduit"
}

variable "connectors_dir" {
  description = "Directory of connector YAML files. One file per integration."
  type        = string
  default     = "../connectors"
}

variable "container_image" {
  description = "Worker image used in the rendered container definition."
  type        = string
  default     = "ghcr.io/say-5/conduit:latest"
}

variable "enable_ecs" {
  description = "Create an ECS task definition per connector (needs a real AWS account or LocalStack Pro)."
  type        = bool
  default     = false
}

variable "ecs_execution_role_arn" {
  description = "Execution role for ECS tasks when enable_ecs is true."
  type        = string
  default     = null
}

variable "use_localstack" {
  type    = bool
  default = false
}

variable "localstack_endpoint" {
  type    = string
  default = "http://localhost:4566"
}
