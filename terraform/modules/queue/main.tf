# A work queue, its dead-letter queue joined by a redrive policy, and a
# quarantine queue. After max_receive_count failed receives SQS moves the
# message to the DLQ; the worker never acknowledges a failed delivery, so
# exhaustion is the only path in. Quarantine is the other direction: the worker
# puts a payload there itself when it fails schema or mapping validation, so the
# two never mix and each is drained by its own command.
resource "aws_sqs_queue" "quarantine" {
  name                      = "${var.name}-quarantine"
  message_retention_seconds = var.dlq_retention_seconds
  sqs_managed_sse_enabled   = true
  tags                      = var.tags
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = var.dlq_retention_seconds
  sqs_managed_sse_enabled   = true
  tags                      = var.tags
}

resource "aws_sqs_queue" "main" {
  name                       = var.name
  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = 20
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main.arn]
  })
}
