"""``conduit`` command line: submit, worker, dlq, quarantine, schema, config."""

from __future__ import annotations

import csv
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Annotated

import structlog
import typer

from conduit.adapters import build_adapter
from conduit.config import ConfigError, ConnectorSpec, load_all
from conduit.core.idempotency import DynamoIdempotencyStore, idempotency_key
from conduit.core.mapping import validate_task
from conduit.core.queue import (
    SqsQueue,
    aws_client,
    ensure_queues,
    list_dead_letters,
    list_quarantined,
    redrive_quarantined,
    replay_dead_letters,
)
from conduit.core.schema import RegistryError, SourceSchema, load_registry, validate_payload
from conduit.models import Envelope, Task
from conduit.ops import StatusStore, collect, render_costs, render_summary

app = typer.Typer(help="Sync tasks to Slack, Jira, and webhooks through one adapter interface.")
dlq_app = typer.Typer(help="Inspect and replay dead letters.")
quarantine_app = typer.Typer(help="Inspect and redrive payloads that failed validation.")
config_app = typer.Typer(help="Validate and inspect connector YAML files.")
schema_app = typer.Typer(help="Inspect the versioned source schema registry.")
queues_app = typer.Typer(help="Create queues locally without Terraform.")
ops_app = typer.Typer(help="Queue depths, worker state, and what a run cost.")
app.add_typer(dlq_app, name="dlq")
app.add_typer(quarantine_app, name="quarantine")
app.add_typer(config_app, name="config")
app.add_typer(schema_app, name="schema")
app.add_typer(queues_app, name="queues")
app.add_typer(ops_app, name="ops")

ConnectorsDir = Annotated[
    Path,
    typer.Option("--connectors-dir", envvar="CONDUIT_CONNECTORS_DIR", help="Directory of YAMLs"),
]
SchemasDir = Annotated[
    Path,
    typer.Option("--schemas-dir", envvar="CONDUIT_SCHEMAS_DIR", help="Directory of source schemas"),
]
TableName = Annotated[
    str, typer.Option("--table", envvar="CONDUIT_TABLE", help="DynamoDB idempotency table")
]


def _configure_logging(json_logs: bool) -> None:
    level = os.environ.get("CONDUIT_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, stream=sys.stderr, format="%(message)s")
    renderer = structlog.processors.JSONRenderer() if json_logs else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level, logging.INFO)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _spec(connectors_dir: Path, name: str) -> ConnectorSpec:
    try:
        specs = load_all(connectors_dir)
    except ConfigError as exc:
        typer.echo(f"config error: {exc}", err=True)
        raise typer.Exit(2) from exc
    if name not in specs:
        typer.echo(f"unknown connector {name!r}; known: {sorted(specs)}", err=True)
        raise typer.Exit(2)
    return specs[name]


def _schema(schemas_dir: Path, connector: str) -> SourceSchema | None:
    try:
        return load_registry(schemas_dir).latest(connector)
    except RegistryError as exc:
        typer.echo(f"schema error: {exc}", err=True)
        raise typer.Exit(2) from exc


def _queue_with_wait(name: str, wait_seconds: int) -> SqsQueue:
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return SqsQueue.by_name(name)
        except Exception as exc:
            if time.monotonic() >= deadline:
                typer.echo(f"queue {name} not available: {exc}", err=True)
                raise typer.Exit(1) from exc
            time.sleep(2)


def _read_tasks(path: Path) -> list[Task]:
    if path.suffix == ".csv":
        with path.open(newline="") as fh:
            rows = list(csv.DictReader(fh))
        tasks = []
        for row in rows:
            row = {k: v for k, v in row.items() if v not in (None, "")}
            if "labels" in row:
                row["labels"] = [x.strip() for x in str(row["labels"]).split("|") if x.strip()]
            tasks.append(Task.model_validate(row))
        return tasks
    text = path.read_text()
    if path.suffix == ".jsonl":
        return [Task.model_validate_json(line) for line in text.splitlines() if line.strip()]
    data = json.loads(text)
    if isinstance(data, dict):
        data = data.get("tasks", [data])
    return [Task.model_validate(item) for item in data]


@app.command()
def submit(
    file: Annotated[Path, typer.Argument(exists=True, help="JSON, JSONL, or CSV of tasks")],
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    schemas_dir: SchemasDir = Path("schemas"),
    repeat: Annotated[int, typer.Option(help="Submit each task this many times")] = 1,
) -> None:
    """Enqueue tasks for one connector; every task gets a deterministic idempotency key.

    Tasks that fail the connector's source schema or mapping rules are reported
    and left out; the command exits 1 when any were rejected so a pipeline
    notices. Nothing reaches the queue, so nothing reaches quarantine either.
    """
    spec = _spec(connectors_dir, connector)
    schema = _schema(schemas_dir, connector)
    tasks = _read_tasks(file)
    accepted = []
    rejected = 0
    for t in tasks:
        problem = validate_payload(schema, t) if schema is not None else None
        problem = problem or validate_task(spec, t)
        if problem is None:
            accepted.append(t)
            continue
        rejected += 1
        typer.echo(f"rejected {t.id} v{t.version}: {problem} ({problem.reason})", err=True)
    queue = SqsQueue.by_name(spec.queue_name)
    envelopes = (
        Envelope(
            task=t,
            connector=spec.name,
            idempotency_key=idempotency_key(spec.name, t.id, t.version),
        )
        for _ in range(repeat)
        for t in accepted
    )
    sent = queue.send_batch(envelopes)
    typer.echo(
        f"submitted {sent} messages ({len(accepted)} tasks x {repeat}) to {spec.queue_name}"
        + (f", {rejected} rejected" if rejected else "")
    )
    if rejected:
        raise typer.Exit(1)


@app.command()
def worker(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    schemas_dir: SchemasDir = Path("schemas"),
    table: TableName = "conduit-idempotency",
    metrics_port: Annotated[int, typer.Option(envvar="CONDUIT_METRICS_PORT")] = 9100,
    max_messages: Annotated[int | None, typer.Option(help="Exit after N messages")] = None,
    idle_polls: Annotated[int | None, typer.Option(help="Exit after N empty polls")] = None,
    wait_seconds: Annotated[int, typer.Option(help="SQS long poll wait")] = 20,
    json_logs: Annotated[bool, typer.Option(envvar="CONDUIT_JSON_LOGS")] = False,
    wait_for_queue: Annotated[
        int, typer.Option(help="Seconds to wait for the queue to exist (Terraform may lag)")
    ] = 0,
) -> None:
    """Consume the connector queue and deliver through its adapter."""
    from conduit import metrics
    from conduit.worker import Worker

    _configure_logging(json_logs)
    spec = _spec(connectors_dir, connector)
    adapter = build_adapter(spec)
    if not adapter.healthcheck():
        typer.echo(f"healthcheck failed for {spec.name}", err=True)
        raise typer.Exit(1)
    store = DynamoIdempotencyStore(
        table, ttl_seconds=spec.idempotency_ttl_seconds, client=aws_client("dynamodb")
    )
    queue = _queue_with_wait(spec.queue_name, wait_for_queue)
    quarantine = _queue_with_wait(spec.quarantine_name, wait_for_queue)
    dlq = _queue_with_wait(spec.dlq_name, wait_for_queue)
    schema = _schema(schemas_dir, connector)
    if metrics_port > 0:
        metrics.serve(metrics_port)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    w = Worker(
        spec,
        adapter,
        store,
        queue,
        quarantine=quarantine,
        dlq=dlq,
        schema=schema,
        statuses=StatusStore(table, client=aws_client("dynamodb")),
    )
    try:
        w.run(
            stop=stop,
            max_messages=max_messages,
            idle_polls=idle_polls,
            wait_seconds=wait_seconds,
        )
    finally:
        adapter.close()
    typer.echo(json.dumps({"connector": spec.name, **w.summary()}))


@dlq_app.command("list")
def dlq_list(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    limit: int = 100,
) -> None:
    """Show dead letters (task id, receive count, key) without consuming them."""
    _configure_logging(json_logs=False)
    spec = _spec(connectors_dir, connector)
    dlq = SqsQueue.by_name(spec.dlq_name)
    messages = list_dead_letters(dlq, limit=limit)
    for m in messages:
        typer.echo(
            json.dumps(
                {
                    "task_id": m.envelope.task.id,
                    "version": m.envelope.task.version,
                    "idempotency_key": m.envelope.idempotency_key,
                    "receive_count": m.receive_count,
                    "replays": m.envelope.attempt,
                }
            )
        )
    typer.echo(f"{len(messages)} dead letters in {spec.dlq_name}", err=True)


@dlq_app.command("replay")
def dlq_replay(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    limit: int | None = None,
) -> None:
    """Move dead letters back onto the connector queue."""
    _configure_logging(json_logs=False)
    spec = _spec(connectors_dir, connector)
    moved = replay_dead_letters(
        SqsQueue.by_name(spec.dlq_name), SqsQueue.by_name(spec.queue_name), limit=limit
    )
    typer.echo(f"replayed {moved} messages from {spec.dlq_name} to {spec.queue_name}")


@quarantine_app.command("list")
def quarantine_list(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    limit: int = 100,
) -> None:
    """Show quarantined payloads and the note that set each one aside."""
    spec = _spec(connectors_dir, connector)
    messages = list_quarantined(SqsQueue.by_name(spec.quarantine_name), limit=limit)
    for m in messages:
        note = m.envelope.quarantine
        typer.echo(
            json.dumps(
                {
                    "task_id": m.envelope.task.id,
                    "version": m.envelope.task.version,
                    "stage": note.stage if note else "",
                    "field": note.field if note else "",
                    "reason": note.reason if note else "",
                    "detail": note.detail if note else "",
                }
            )
        )
    typer.echo(f"{len(messages)} quarantined in {spec.quarantine_name}", err=True)


@quarantine_app.command("redrive")
def quarantine_redrive(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
    limit: int | None = None,
) -> None:
    """Move quarantined payloads back onto the connector queue after a fix."""
    spec = _spec(connectors_dir, connector)
    moved = redrive_quarantined(
        SqsQueue.by_name(spec.quarantine_name), SqsQueue.by_name(spec.queue_name), limit=limit
    )
    typer.echo(f"redrove {moved} messages from {spec.quarantine_name} to {spec.queue_name}")


@schema_app.command("check")
def schema_check(schemas_dir: SchemasDir = Path("schemas")) -> None:
    """Load every version and report the registry; exits 2 on a breaking change."""
    try:
        registry = load_registry(schemas_dir)
    except RegistryError as exc:
        typer.echo(f"breaking: {exc}", err=True)
        raise typer.Exit(2) from exc
    names = registry.connectors()
    if not names:
        typer.echo(f"no schemas in {schemas_dir}")
        return
    for name in names:
        history = registry.history(name)
        latest = history[-1]
        typer.echo(
            f"ok  {name:<16} versions=1..{latest.version} "
            f"fields={len(latest.fields)} "
            f"required={sum(1 for f in latest.fields.values() if f.required)}"
        )


@ops_app.command("summary")
def ops_summary(
    connectors_dir: ConnectorsDir = Path("connectors"),
    table: TableName = "conduit-idempotency",
) -> None:
    """Queue, dead-letter, and quarantine depths per connector with each worker's state."""
    specs = load_all(connectors_dir)
    statuses = StatusStore(table, client=aws_client("dynamodb"))
    typer.echo(render_summary(collect(specs, aws_client("sqs"), statuses)))


@ops_app.command("costs")
def ops_costs(
    connectors_dir: ConnectorsDir = Path("connectors"),
    table: TableName = "conduit-idempotency",
) -> None:
    """Objects and items each worker wrote during its current run, priced per million."""
    specs = load_all(connectors_dir)
    statuses = StatusStore(table, client=aws_client("dynamodb"))
    typer.echo(render_costs(collect(specs, aws_client("sqs"), statuses)))


@config_app.command("validate")
def config_validate(
    connectors_dir: ConnectorsDir = Path("connectors"),
    schemas_dir: SchemasDir = Path("schemas"),
) -> None:
    """Parse every YAML and report each connector's type, target, and queue names."""
    try:
        specs = load_all(connectors_dir)
    except ConfigError as exc:
        typer.echo(f"invalid: {exc}", err=True)
        raise typer.Exit(2) from exc
    for spec in specs.values():
        schema = _schema(schemas_dir, spec.name)
        typer.echo(
            f"ok  {spec.name:<16} {spec.type:<8} target={spec.target} "
            f"queue={spec.queue_name} dlq={spec.dlq_name} "
            f"quarantine={spec.quarantine_name} "
            f"max_receive={spec.queue.max_receive_count} attempts={spec.retry.max_attempts} "
            f"mapping={len(spec.mapping)} schema={f'v{schema.version}' if schema else 'none'}"
        )


@config_app.command("show")
def config_show(connectors_dir: ConnectorsDir = Path("connectors")) -> None:
    """Dump the resolved specs as JSON (defaults filled in)."""
    try:
        specs = load_all(connectors_dir)
    except ConfigError as exc:
        typer.echo(f"invalid: {exc}", err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps({n: s.model_dump(mode="json") for n, s in specs.items()}, indent=2))


@queues_app.command("ensure")
def queues_ensure(
    connectors_dir: ConnectorsDir = Path("connectors"),
    table: TableName = "conduit-idempotency",
) -> None:
    """Create queues, DLQs, and the idempotency table directly (no Terraform)."""
    specs = load_all(connectors_dir)
    sqs = aws_client("sqs")
    for spec in specs.values():
        q, d, quarantine = ensure_queues(spec, sqs)
        typer.echo(f"{spec.name}: {q.queue_url} -> {d.queue_url}, {quarantine.queue_url}")
    DynamoIdempotencyStore(table, client=aws_client("dynamodb")).ensure_table()
    typer.echo(f"table {table} ready")


if __name__ == "__main__":
    app()
