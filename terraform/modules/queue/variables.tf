variable "name" {
  type = string
}

variable "max_receive_count" {
  type    = number
  default = 3
}

variable "visibility_timeout_seconds" {
  type    = number
  default = 60
}

variable "message_retention_seconds" {
  type    = number
  default = 345600
}

variable "dlq_retention_seconds" {
  type    = number
  default = 1209600
}

variable "tags" {
  type    = map(string)
  default = {}
}
