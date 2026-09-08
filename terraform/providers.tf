# One provider block serves both targets. With use_localstack = true every
# service endpoint points at LocalStack and credential checks are skipped;
# with the default (false) the same modules deploy to a real AWS account.
provider "aws" {
  region = var.aws_region

  access_key = var.use_localstack ? "test" : null
  secret_key = var.use_localstack ? "test" : null

  skip_credentials_validation = var.use_localstack
  skip_metadata_api_check     = var.use_localstack
  skip_requesting_account_id  = var.use_localstack
  s3_use_path_style           = var.use_localstack

  dynamic "endpoints" {
    for_each = var.use_localstack ? [var.localstack_endpoint] : []
    content {
      sqs      = endpoints.value
      dynamodb = endpoints.value
      iam      = endpoints.value
      ssm      = endpoints.value
      sts      = endpoints.value
      ecs      = endpoints.value
    }
  }

  default_tags {
    tags = {
      Project   = "conduit"
      ManagedBy = "terraform"
    }
  }
}
