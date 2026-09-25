# Conduit

Reusable integration connector kit. One adapter interface syncs tasks to Slack, Jira, and
signed webhooks, with idempotency keys, exponential backoff, a token-bucket throttle and
circuit breaker per connector, versioned source schemas, and an SQS dead-letter queue beside
a quarantine queue for payloads that will never be valid. Terraform reads the `connectors/`
directory, so a new integration ships from one YAML file and one `terraform apply`.

Python 3.12, boto3, httpx, pydantic v2, Terraform 1.5, Docker. Runs against LocalStack locally
and in CI; the same modules target a real AWS account by dropping the endpoint overrides.

## Architecture

```
 conduit submit tasks.json -c jira-support
        |
        v
 +---------------------+     redrive after      +--------------------------+
 | SQS conduit-<name>  | ---- maxReceiveCount --> | SQS conduit-<name>-dlq   |
 +---------------------+                         +--------------------------+
        |  long poll, visibility extension                 ^   conduit dlq list | replay
        v                                                  |
 +---------------------------------------------------------+---------------+
 | worker (one per connector)                                              |
 |   check(source schema) : versioned registry, first violation wins       |
 |   check(mapping rules) : required/type/enum/length                      |
 |     either failure ----> SQS conduit-<name>-quarantine {stage, reason}  |
 |     conduit quarantine list | redrive                                   |
 |   claim(idempotency key) ----> DynamoDB conditional PutItem + TTL       |
 |   throttle(token bucket) : rate + burst, Retry-After pauses the source  |
 |   breaker(target failures) : stop polling, hold visibility, one probe   |
 |   retry_call(adapter.deliver) : 429/5xx/timeout retried, 4xx returned   |
 |   /metrics : delivered, deduplicated, quarantined, retried, dead_letter |
 +----------------+----------------------+---------------------------+----+
                  |                      |                           |
                  v                      v                           v
           SlackAdapter            JiraAdapter               WebhookAdapter
        chat.postMessage        REST v3 issue upsert       HMAC-SHA256 signed POST
        Block Kit + metadata    summary tag, no JQL        Idempotency-Key header

 connectors/*.yaml        --fileset + yamldecode + for_each--> terraform (queue, dlq,
 schemas/<name>/v<N>.yaml --a registry that refuses a------->  quarantine, redrive, IAM
                            breaking change at load time      policy/role, SSM params,
                                                              container definition)
```

Idempotency key = `sha256("v1|connector|task_id|task_version")`. A resubmit of the same task
revision is short-circuited by the worker as `deduplicated`; a new revision updates the remote
object recorded for the previous one (Jira issue key, for example). Details in
[ARCHITECTURE.md](ARCHITECTURE.md).

## One config file per integration

`connectors/jira-support.yaml`:

```yaml
type: jira
target: SUP
base_url: ${JIRA_BASE_URL:-https://example.atlassian.net}
secrets:
  email: JIRA_EMAIL
  api_token: JIRA_API_TOKEN
mapping:
  summary:
    source: title
    required: true
    max_length: 255          # truncated to fit
  description: body
  issuetype:
    default: Task            # constant
  customfield_10042:
    source: id
    required: true
retry:
  max_attempts: 4
  base_seconds: 0.25
  max_seconds: 8
rate_limit:
  requests_per_second: 10
  burst: 5
  max_retry_after_seconds: 120
breaker:
  failure_threshold: 5     # consecutive 5xx or timeouts
  recovery_seconds: 30
queue:
  max_receive_count: 3
  visibility_timeout_seconds: 45
```

A mapping value is either a bare source (`description: body`, a dotted task path or a
`$`-template such as `"[$priority] $title"`) or a rule with `source`, `type` (`string`,
`integer`, `number`, `boolean`, `list`, or `any`), `required`, `enum`, `default`,
`max_length`, and `truncate`. Defaults fill missing values, types are coerced, and an
overlong value is cut or rejected depending on `truncate`. The worker checks every task
against the rules before it claims an idempotency key: a task that cannot fit is moved to the
connector's quarantine queue with the field and reason, and counted in
`conduit_quarantined_total{stage,reason}`, instead of cycling through retries into the DLQ.
`conduit submit` applies the same rules and refuses the offending tasks up front.

Malformed queue envelopes are logged by message ID without their contents and left
unacknowledged for the queue's configured dead-letter redrive policy. Other valid
messages in the same response continue normally. If provided, `submitted_at` must
include a timezone, for example `2026-09-08T12:00:00Z`. The typed DLQ CLI only lists
and replays valid envelopes; inspect malformed bodies through SQS tooling and correct
them before resubmitting.

The worker reads the same file to build the adapter, retry policy, rate limit, and breaker.
Terraform reads it to create the three queues with the redrive policy, a least-privilege IAM
policy and role, one SSM SecureString placeholder per secret, and a container definition.
Adding a fourth connector is one file:

```yaml
# connectors/pager-oncall.yaml
type: slack
target: "#oncall"
secrets:
  token: PAGER_SLACK_TOKEN
```

```
$ terraform -chdir=terraform plan -var-file=localstack.tfvars
Plan: 8 to add, 0 to change, 0 to destroy.
```

`tests/terraform/test_terraform.py` asserts that the plan diff for a new YAML is exactly those
eight resources.

## Source schemas and quarantine

Mapping rules describe the remote side of a connector. A schema describes the source side: what
the producer promised to send. They live in `schemas/<connector>/v<N>.yaml` and are loaded as an
ordered registry.

```yaml
# schemas/jira-support/v2.yaml
fields:
  id:
    type: string
    required: true
    max_length: 256
  title:
    type: string
    required: true
  priority:
    type: string
    enum: [low, normal, high, urgent]   # v1 had no urgent; widening is compatible
  fields.region:
    type: string
  fields.reporter:                      # new and optional, so v1 payloads still fit
    type: string
```

Loading refuses a breaking change between consecutive versions. A new version may loosen what
it accepts and never tighten it, because tightening would quarantine payloads the previous
version let through: making a field required, narrowing its type, introducing an enum or
dropping values from one, and introducing or lowering a `max_length` are all refused. Dropping
a field, relaxing `required`, widening to `any` or `integer` to `number`, adding enum values,
and adding an optional field are all accepted.

```
$ conduit schema check
ok  jira-support     versions=1..2 fields=5 required=2
ok  slack-ops        versions=1..1 fields=4 required=2
ok  webhook-crm      versions=1..1 fields=4 required=3
```

A payload that fails the schema, or the mapping rules after it, is moved to
`conduit-<name>-quarantine` carrying a note with the stage, field, reason, and detail. That
queue is deliberately not the dead-letter queue: the DLQ holds deliveries the handler could
not complete after `maxReceiveCount` receives and is replayed once the target is healthy,
while quarantine holds payloads that will never be delivered as they stand and is redriven
once the producer or the schema is fixed. Neither ever feeds the other.

```
$ conduit quarantine list -c jira-support
{"task_id": "T-91", "version": 1, "stage": "schema", "field": "priority", "reason": "enum", "detail": "priority: 'critical' is not one of ['low', 'normal', 'high', 'urgent']"}
1 quarantined in conduit-jira-support-quarantine
$ conduit quarantine redrive -c jira-support
redrove 1 messages from conduit-jira-support-quarantine to conduit-jira-support
```

`redrive` moves what the queue held when it started, not what arrives while it runs: a worker
that still rejects the payload quarantines it again inside the call, and an unbounded drain
would chase those forever. Run it after the fix, not before.

A redriven message keeps its idempotency key and loses its note, so a task that was quarantined
and then fixed is still delivered exactly once. `conduit submit` applies the same schema before
anything is enqueued, so a bad file is rejected at the door rather than quarantined later.

## Backpressure and rate control

Every connector carries its own token bucket. `acquire` reserves a token and returns the wait
the worker must sleep before the send is allowed, so the burst goes out immediately and the
rest are spaced `1 / requests_per_second` apart. Tokens go negative while reservations are
outstanding, which keeps the spacing exact across a backlog instead of letting a batch escape
in one go. A 429 with `Retry-After` calls `penalize`, which moves the earliest allowed send
for the whole connector into the future, capped by `max_retry_after_seconds`; the pause
applies to every task on that connector, not only the one that was throttled.

A remote that is down is a different problem from one that is busy, so 5xx responses,
timeouts, and connection errors feed a per-connector circuit breaker while 429 does not.
After `failure_threshold` consecutive target failures the breaker opens and the worker stops
polling entirely. Messages it is already holding stay invisible: `_pause_while_open` keeps
extending their visibility timeout for as long as the pause lasts, so nothing is redelivered
to a second consumer and nothing burns receive count into the DLQ because the target had an
outage. After `recovery_seconds` the breaker half-opens and admits exactly one probe. The
probe succeeding closes it and polling resumes; the probe failing opens it for another
window.

Both are visible on `/metrics` as `conduit_rate_limit_waits_total`,
`conduit_rate_limit_wait_seconds_total`, `conduit_retry_after_honored_total`,
`conduit_breaker_state` (0 closed, 1 half open, 2 open), `conduit_breaker_opens_total`, and
`conduit_breaker_paused_seconds_total`, and in the worker's `worker.stop` log line.

`tests/integration/test_backpressure.py` proves the three behaviours against LocalStack: six
sends at five per second arrive over a one-second window, an outage on a fake pauses the
worker so it consumes nothing and then drains all three tasks once the fault clears, and a
message held across a pause is never handed to a second receiver even though the queue's
visibility timeout is shorter than the pause.

## Operations

Every worker writes a status row into the idempotency table under `status|<connector>` every
few seconds: what its current run has delivered, deduplicated, quarantined, and dead lettered,
how many SQS requests and DynamoDB items it has spent, its breaker state, the tokens left in
its bucket, and its last error. `conduit ops summary` joins those rows to live queue depths
read from SQS, so it needs to know neither where the workers run nor which port they serve
metrics on. A connector whose worker has published nothing reads `no worker seen`, which is how
one that never started is told apart from one that is merely idle.

`conduit ops costs` reports what a run actually bought. The counters are per run rather than
cumulative: each worker records where the store and queue counters stood when it started and
reports the difference, so restarting a worker starts a new bill.

Each worker also serves both views on `/metrics`, beside the delivery counters from earlier
versions. `conduit_queue_depth{connector,queue,visibility}` covers the worker's own work,
dead-letter, and quarantine queues: `queue` is `work`, `dlq`, or `quarantine` and `visibility`
is `visible` or `in_flight`, the same labels `conduit ops summary` sets. The worker refreshes
it after a poll at most once every 10 seconds, so on the default 20 second long poll an idle
worker refreshes once per poll, and each refresh is three `GetQueueAttributes` calls counted in
the run's SQS requests. `conduit_billable_units{connector,service,unit}` is set each time the
status row is written. `make demo` prints both blocks at the end of its run, and the real
output is under [make demo](#make-demo).

`tests/integration/test_ops.py` seeds a connector with three delivered tasks, one dead letter,
one quarantined payload, and one still queued, then asserts those exact depths, the exact item
counts behind the bill, and the rendered lines. It also runs a worker with its own `/metrics`
port and checks that dead letters and a quarantined payload enqueued after the worker started
show up in `conduit_queue_depth`.

## Quick start

```
make setup          # uv sync
make test-unit      # 168 tests, no Docker
make up             # LocalStack + fakes + one worker per connector
make tf-apply       # terraform apply against LocalStack (26 resources)
make test           # unit + LocalStack integration + terraform plan tests
make demo           # everything below
make demo-down
```

Requirements: Python 3.12 with [uv](https://docs.astral.sh/uv/), Docker with Compose, Terraform
1.5 or newer.

The interactive web demo in `web/` uses Node.js 20.19+ or 22.12+ with Vite 7.
From that directory, run `npm ci`, `npm run build`, and `npm run selfcheck`.
CI runs the build and simulation self-check on Node.js 22.

## make demo

The demo submits 300 synthetic tasks (240 unique plus 60 duplicate resubmits, interleaved)
across the three connectors with failure injection on: the Jira fake returns 429 for the first
two attempts of 30 tasks and the webhook fake hard-fails 10 tasks with 400. It then clears the
webhook fault, replays the dead letters, and plans a fourth connector. Output from a real run:

```
conduit demo summary (run DF15F0D, LocalStack at http://localhost:4566)
========================================================================
tasks submitted        300  (240 unique + 60 duplicate resubmits)
deduplicated           60  (must equal duplicates: ok)
delivered per connector
  jira-support     80 delivered,  80 unique keys,  20 deduplicated (jira fake inbox)
  slack-ops        80 delivered,  80 unique keys,  20 deduplicated (slack fake inbox)
  webhook-crm      70 delivered,  70 unique keys,  20 deduplicated (webhook fake inbox)
retried                60  (jira fake returned 429 60 times for 30 tasks)
  backoff evidence     attempt 1 -> 30 retries, attempt 2 -> 30 retries; delay min/median/max 0.019s / 0.154s / 0.497s (policy base 0.25s x2, cap 8s, full jitter)
dead-lettered          10  (must equal hard failures 10: ok); conduit-webhook-crm-dlq after maxReceiveCount=2
  dead letters         0001, 0002, 0003, 0004, 0005, 0006, 0007, 0008, 0009, 0010
DLQ replay             10 replayed after clearing the fault; DLQ now 0; webhook delivered 80/80 in 2.0s
delivery latency       p50 2.0 ms, p95 354.5 ms (worker attempt-to-ack, n=230)
end-to-end latency     p50 3.32 s, p95 13.24 s (submit-to-remote-receipt, n=230); queues drained in 16.4s
new integration from one file (connectors/pager-oncall.yaml, 10 lines):
  Plan: 8 to add, 0 to change, 0 to destroy.
  + module.connector["pager-oncall"].aws_iam_policy.worker
  + module.connector["pager-oncall"].aws_iam_role.worker
  + module.connector["pager-oncall"].aws_iam_role_policy_attachment.worker
  + module.connector["pager-oncall"].aws_ssm_parameter.secret["token"]
  + module.connector["pager-oncall"].module.queue.aws_sqs_queue.dlq
  + module.connector["pager-oncall"].module.queue.aws_sqs_queue.main
  + module.connector["pager-oncall"].module.queue.aws_sqs_queue.quarantine
  + module.connector["pager-oncall"].module.queue.aws_sqs_queue_redrive_allow_policy.dlq

conduit ops summary
  connector         queue inflight   dlq  quar  delivered  per min  throttle  breaker  last error
  -----------------------------------------------------------------------------------------------
  jira-support          0        0     0     0         80     55.2   5.0 tok   closed
  slack-ops             0        0     0     0         80     49.5   5.0 tok   closed
  webhook-crm           0        0     0     0         80     51.3  10.0 tok   closed  permanent DF15F0D-webhook-crm-0006: HTTP 400: {"error":"injected","reason":"hard_fail"}
  -----------------------------------------------------------------------------------------------
  total                 0        0     0     0        240

conduit ops costs
  connector              run  objects  sqs req  ddb write  ddb read       usd
  ---------------------------------------------------------------------------
  jira-support       BF53F08       80      188        260       100    0.0004
  slack-ops          6A4C8A1       80      129        260       100    0.0004
  webhook-crm        05A284F       80      153        300       100    0.0005
  ---------------------------------------------------------------------------
  total                           240      470        820       300    0.0013
  priced at us-east-1 on-demand list: sqs_requests $0.4/M, dynamodb_writes $1.25/M, dynamodb_reads $0.25/M
========================================================================
```

Every figure is read back from the queues, the fakes' inboxes (which record idempotency keys),
the workers' JSON logs and `/metrics`, and a real `terraform plan`. The script exits non-zero
if deduplicated != duplicates, dead letters != hard failures, or the replay does not drain the
DLQ. The end-to-end p95 is dominated by the Jira connector's 10 requests/s rate limit plus its
retries; per-attempt delivery latency is the worker's own measurement.

## Browser demo

`web/` is a static page that runs the same demo without Docker: `web/src/sim` ports the worker,
idempotency store, queue, retry policy, token bucket, and Terraform resource set to TypeScript,
driven by a seeded PRNG and a virtual clock. `npm run selfcheck` in `web/` reproduces the
figures above (60 deduplicated, 10 dead-lettered then replayed to 0, 8 resources planned for
a fourth connector YAML, 26 for the shipped three) and checks that a malformed payload is
quarantined rather than dead-lettered, as 44 assertions in Node. See
[web/README.md](web/README.md).

## LocalStack, not AWS

No AWS account was used to build or verify this project. `deploy/docker-compose.yml` runs
[LocalStack](https://github.com/localstack/localstack) (real SQS, DynamoDB, IAM, and SSM APIs)
and Terraform applies against it through the provider endpoint overrides in
`terraform/localstack.tfvars`. Nothing was deployed to a real AWS account. To target AWS, omit
`-var-file=localstack.tfvars` and provide normal credentials; the modules are unchanged. ECS
task definitions are rendered as outputs and only registered with `enable_ecs = true`, since
ECS is not in LocalStack's community edition.

## CLI

```
conduit submit FILE -c NAME [--repeat N]   enqueue tasks from JSON, JSONL, or CSV
conduit worker -c NAME [--metrics-port 9100] [--max-messages N] [--idle-polls N]
conduit dlq list -c NAME [--limit N]       peek at dead letters without consuming them
conduit dlq replay -c NAME [--limit N]     move dead letters back to the work queue
conduit quarantine list -c NAME [--limit N]     payloads set aside, with the note that did it
conduit quarantine redrive -c NAME [--limit N]  put them back after the fix
conduit schema check [--schemas-dir DIR]   load the registry, exit 2 on a breaking change
conduit ops summary                        queue depths and worker state per connector
conduit ops costs                          objects and items written by each current run
conduit config validate [--connectors-dir DIR]
conduit config show                        resolved specs as JSON
conduit queues ensure                      create queues and table directly, without Terraform
```

Environment: `CONDUIT_CONNECTORS_DIR` (default `connectors`), `CONDUIT_SCHEMAS_DIR` (default
`schemas`), `CONDUIT_TABLE` (default `conduit-idempotency`), `CONDUIT_AWS_ENDPOINT_URL`
(LocalStack), `CONDUIT_JSON_LOGS`, plus the secret variables each YAML names. `${VAR:-default}` in a YAML value is expanded from the
environment; the demo uses that to point `base_url` and `target` at the fakes.

## Layout

```
conduit/            package: adapters/, core/ (idempotency, retry, queue, ratelimit, breaker,
                    schema), worker, ops, cli, metrics
connectors/         one YAML per integration
schemas/            one directory per connector, one file per schema version
terraform/          root with for_each over connectors/, modules/{queue,idempotency-table,connector}
fakes/              FastAPI stand-ins for Slack, Jira, and a webhook receiver (fault injection, inbox)
deploy/             docker-compose.yml (LocalStack, fakes, workers, optional Prometheus)
demo/run.py         the end-to-end run behind make demo
tests/              unit (respx, moto), integration (LocalStack), terraform (plan diff)
```

## Changelog

### v5.0.1

* Workers now set `conduit_queue_depth{connector,queue,visibility}` for their own work,
  dead-letter, and quarantine queues, refreshed after a poll at most once every 10 seconds
  (`QUEUE_DEPTH_INTERVAL_SECONDS`). In v5.0.0 only `conduit ops` set it, so a scrape of a
  worker did not show it.
* `tests/unit/test_worker.py` checks the refresh and its interval against a stubbed SQS
  client, and `tests/integration/test_ops.py` scrapes a running worker's `/metrics` after
  enqueueing.
* The browser demo plans the per-connector quarantine queue: 8 resources for a new connector
  and 26 for the shipped base stack, as the real plan does. Its DLQ lab sends a malformed
  payload to quarantine, and the self-check runs 44 assertions.
* ARCHITECTURE.md and CONTRIBUTING.md quote the current plan counts.

### v5.0.0

* Every worker publishes a status row into the idempotency table under `status|<connector>`:
  run id, counts, breaker state, tokens left in the bucket, and last error, refreshed at most
  every five seconds and once more on stop.
* `conduit ops summary` joins those rows to live SQS depths and prints queue, dead-letter, and
  quarantine depths with per-connector throughput, throttle state, breaker state, and last
  error. A connector with no published row reads `no worker seen`.
* `conduit ops costs` reports remote objects, SQS requests, and DynamoDB writes and reads for
  the current run, priced at the us-east-1 on-demand list rates in `PRICE_PER_MILLION`.
  Counters are per run: each worker subtracts the baseline it saw at startup.
* `conduit_queue_depth{connector,queue,visibility}` and
  `conduit_billable_units{connector,service,unit}` join the existing metrics. As released,
  only `conduit ops` set the queue depth gauge; workers set it from v5.0.1.
* `make demo` prints both ops blocks and writes the collected rows into
  `demo/out/details.json`.

### v4.0.0

* Per-connector source schemas in `schemas/<connector>/v<N>.yaml`, loaded as a versioned
  registry. Fields are checked as they arrive, never coerced, and the first violation wins.
* Loading refuses a breaking change between consecutive versions: making a field required,
  narrowing a type, introducing or shrinking an enum, and introducing or lowering a
  `max_length` are all rejected. `conduit schema check` reports the registry and exits 2 on a
  break.
* A third queue per connector, `conduit-<name>-quarantine`, created by Terraform alongside the
  work queue and the DLQ. Payloads that fail the schema or the mapping rules go there with a
  note recording the stage, field, reason, and detail, instead of being acknowledged and
  dropped as they were in v2.
* `conduit quarantine list` and `conduit quarantine redrive`; a redriven message keeps its
  idempotency key and loses its note. `redrive` is bounded by the depth it saw when it started
  so it cannot chase re-quarantined messages.
* `conduit_rejected_total{reason}` is replaced by `conduit_quarantined_total{stage,reason}`,
  the worker stat `rejected` by `quarantined`, and `DeliveryStatus.REJECTED` by
  `QUARANTINED`. `conduit submit` still rejects at the door, which is the case where nothing
  was ever enqueued.

### v3.0.0

* Token-bucket throttle per connector: `rate_limit.requests_per_second`, `burst`, and
  `max_retry_after_seconds`. A 429 with `Retry-After` pauses every send on that connector,
  not only the message that was throttled.
* Circuit breaker per connector: `breaker.failure_threshold` consecutive 5xx, timeouts, or
  connection errors stop the worker polling; after `breaker.recovery_seconds` one probe
  decides whether it resumes. 429 feeds the bucket, not the breaker.
* Messages held when the breaker opens keep their visibility extended for the length of the
  pause, so an outage cannot cause a redelivery to a second consumer or burn receive count
  into the dead-letter queue.
* `conduit_rate_limit_waits_total`, `conduit_rate_limit_wait_seconds_total`,
  `conduit_retry_after_honored_total`, `conduit_breaker_state`,
  `conduit_breaker_opens_total`, and `conduit_breaker_paused_seconds_total` on `/metrics`.
* The fakes take an `outage` fault that fails every call with 503 until it is cleared.

### v2.0.0

* Connector YAML mappings are typed rules: `source`, `type`, `required`, `enum`, `default`,
  `max_length`, `truncate`. A bare string is still a source-only rule.
* Transforms run before delivery: `$`-templates, defaults and constants, type coercion, and
  truncation.
* The worker validates every task before claiming its idempotency key and rejects misfits
  with the field and reason; `conduit_rejected_total{reason}` and the `rejected` stat count
  them. `conduit submit` rejects the same tasks before they are enqueued and exits 1.
* `conduit config validate` reports the number of mapping rules per connector.

### v1.0.0

* One adapter interface for Slack, Jira, and signed webhooks.
* Idempotency keys claimed through DynamoDB conditional puts, full-jitter backoff retries,
  SQS queue plus DLQ per connector with `conduit dlq list` and `conduit dlq replay`.
* Prometheus metrics per worker; Terraform `for_each` over `connectors/*.yaml`; LocalStack
  for local runs and CI; `make demo` end-to-end run.

## License

MIT
