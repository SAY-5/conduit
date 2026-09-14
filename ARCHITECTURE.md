# Architecture

## Adapter interface

`conduit/adapters/base.py` defines the single contract every integration implements:

```python
class Adapter(abc.ABC):
    def deliver(self, task: Task, idempotency_key: str, remote_id: str | None = None) -> DeliveryResult
    def healthcheck(self) -> bool
```

An adapter owns one `httpx.Client` and one `ConnectorSpec` (the parsed YAML: type, target,
secret env var names, field mapping, retry policy, rate limit, queue settings). It raises
`TransientError` for retryable failures and `PermanentError` for everything else; it never
sleeps, loops, or touches the queue. `remote_id` is the identifier recorded when a previous
revision of the same task was delivered, so an adapter can update instead of create.

Field mappings are `remote_field: rule`. A rule is a bare source, either a dotted path into the
task (`fields.region`) or a `$`-template (`"[$priority] $title"`), or a mapping with `source`,
`type`, `required`, `enum`, `default`, `max_length`, and `truncate`. `conduit/core/mapping.py`
resolves the source, falls back to `default` (a rule with only a default is a constant), coerces
to `type` (`any` keeps the task's own type; `integer`, `number`, `boolean`, and `list` parse
strings), then checks `required`, `enum`, and `max_length`, cutting or rejecting an overlong
value. The first violation raises `MappingError(field, reason)`, where `reason` is one of
`required`, `type`, `enum`, `max_length`. Adapters call `mapped(task)` and receive the
transformed fields. Secrets are looked up by logical name through the env var named in the
spec, so a YAML never contains a credential.

| Adapter | Transport | Idempotency on the remote side |
|---|---|---|
| `SlackAdapter` | `chat.postMessage` with Block Kit, or an incoming webhook URL as `target` | key in message `metadata.event_payload` and `Idempotency-Key` header; `ok: false` errors classified (`ratelimited` transient, others permanent) |
| `JiraAdapter` | REST v3 `POST /issue`, `PUT /issue/{key}` | summary tag `[conduit:<task id>]` plus an optional custom field; updates go to the recorded issue key, never a JQL search; a 404 on update falls back to create |
| `WebhookAdapter` | JSON POST | `Idempotency-Key`, `X-Conduit-Timestamp`, and `X-Conduit-Signature: sha256=HMAC(secret, "<ts>." + body)` |

## Idempotency

Key: `sha256("v1|<connector>|<task id>|<task version>")`, computed at submit time and carried
in the SQS message body and attributes.

Store: one DynamoDB table (`pk` string, TTL on `expires_at`). The worker claims a key before
delivering:

```
PutItem(pk=key, state=in_progress, lease_until=now+lease, expires_at=now+ttl)
  ConditionExpression: attribute_not_exists(pk) OR (state = in_progress AND lease_until < now)
```

* Condition passes: this worker owns the key and delivers.
* Condition fails and the item is `delivered`: duplicate; the message is acknowledged and
  counted as `deduplicated` without touching the remote system.
* Condition fails and the item is `in_progress` with a live lease: another worker is on it; the
  message is re-polled after 10 seconds.
* Delivery succeeds: `UpdateItem` sets `state=delivered, remote_id=...` and writes a
  `latest|<connector>|<task id>` pointer so the next revision can update the same remote object.
* Delivery fails: the claim is released (conditional `DeleteItem` on `state = in_progress`) so
  the SQS redrive, or a later DLQ replay, can claim it again. A delivered record is never
  released.

`MemoryIdempotencyStore` implements the same semantics for the worker unit tests; the DynamoDB
store is tested with moto and against LocalStack.

## Retry classification and backoff

`conduit/core/retry.py`:

* `classify_response`: 2xx is success; status in `retry.retry_on_status` (default 408, 425, 429,
  500, 502, 503, 504) or any 5xx is `TransientError` with `Retry-After` parsed; any other 4xx is
  `PermanentError`.
* `classify_exception`: httpx timeouts and network errors are transient; anything else is
  permanent.
* `backoff_delay(attempt)`: full jitter, `uniform(0, min(max_seconds, base * multiplier^(attempt-1)))`.
  A `Retry-After` header raises the floor for that attempt but never exceeds `max_seconds`.
* `retry_call`: runs the delivery up to `max_attempts` times inside one SQS receive, sleeping
  between attempts. Before each sleep the worker extends the message's visibility timeout so
  the message cannot be redelivered mid-retry. When attempts are exhausted the last transient
  error propagates.

Permanent errors and exhausted transient errors take the same path in the worker: release the
claim, count `failed{reason=permanent|exhausted}`, and set the message's visibility to 0 so SQS
redelivers it immediately. There is no in-process retry for permanent errors; a fixed
integration is handled by replaying from the DLQ.

## Dead-letter queue and replay

Every connector gets `conduit-<name>` and `conduit-<name>-dlq`, joined by a redrive policy with
`maxReceiveCount` from the YAML (`queue.max_receive_count`). Because the worker only deletes a
message after a successful delivery or a confirmed duplicate, a message that fails
`maxReceiveCount` times is moved to the DLQ by SQS itself. The worker counts `dead_lettered`
when it fails a message whose `ApproximateReceiveCount` has reached the limit.

`conduit dlq list` peeks (receive with a short visibility window, then hands visibility back)
and prints task id, version, key, receive count, and how many times the message was replayed.
`conduit dlq replay` moves messages back to the work queue with `attempt + 1` in the envelope
and deletes them from the DLQ; the fresh message gets a fresh receive count. The idempotency
claim was released on failure, so a replay after the integration is fixed delivers normally.

## Worker

`conduit/worker.py` is a single loop per connector: long-poll receive (up to 10 messages, 20 s
wait), then for each message: record queue lag, validate the task against the mapping rules
(a misfit is deleted from the queue, logged as `delivery.rejected` with field and reason, and
never claims a key), claim, resolve `remote_id` for revisions above 1, throttle to the spec's
`requests_per_second`, `retry_call(adapter.deliver)`, then either mark delivered and delete, or
release and set visibility to 0. Metrics on `/metrics` (Prometheus client):
`conduit_delivered_total`, `conduit_deduplicated_total`, `conduit_quarantined_total{stage,reason}`,
`conduit_retried_total`, `conduit_failed_total{reason}`, `conduit_dead_lettered_total`,
`conduit_delivery_seconds` (attempt to ack), `conduit_queue_lag_seconds` (submit to
receive), and `conduit_queue_depth{queue,visibility}` for the connector's work, dlq, and
quarantine queues (refreshed after a poll, at most once every 10 seconds), all labelled by
connector. Logs are structlog, JSON when `CONDUIT_JSON_LOGS=1`.

## Terraform layout

```
terraform/
  main.tf            fileset(connectors_dir, "*.yaml") -> yamldecode -> module.connector for_each
  providers.tf       one aws provider; use_localstack switches endpoints and credential checks
  localstack.tfvars  use_localstack = true, localstack_endpoint = http://localhost:4566
  modules/
    queue/               aws_sqs_queue main + dlq + quarantine, redrive_policy, redrive_allow_policy
    idempotency-table/   aws_dynamodb_table with TTL on expires_at, PITR
    connector/           module.queue + aws_ssm_parameter per secret + IAM policy/role/attachment
                         + container definition (aws_ecs_task_definition when enable_ecs)
```

Per connector the plan is 7 resources plus one SSM parameter per secret; the table is shared.
The YAML is the only input: queue settings come from `queue.*`, secrets from `secrets`, and the
container definition wires `CONDUIT_CONNECTORS_DIR`, `CONDUIT_TABLE`, and one
`valueFrom` SSM reference per secret. `tests/terraform/test_terraform.py` runs `fmt`,
`validate`, plans the shipped connectors (26 creates), and proves that adding one YAML changes
the plan by exactly that connector's 8 resources (7 plus its one secret).

Against LocalStack the queue URLs use the `standard` endpoint strategy
(`http://sqs.<region>.localhost.localstack.cloud:4566/<account>/<name>`), which is the format
the AWS provider parses; both boto3 and the provider send requests to the configured endpoint
and pass the URL as a parameter, so the hostname is never resolved.

## Fakes

`fakes/` holds three FastAPI apps that speak enough of each API for the adapters: Slack
(`auth.test`, `chat.postMessage` with `ok: false` errors), Jira (`myself`, create/update/get
issue, summary-tag parsing), and a webhook receiver that verifies the HMAC signature and dedupes
on `Idempotency-Key`. Each exposes `GET/DELETE /_inbox` (everything received, with keys) and
`GET/POST/DELETE /_faults` for injection: 429 for the first N calls of listed tasks, 400 for
listed tasks, or one 500 per task. The integration tests run them in-process on random ports;
the demo runs them as containers.
