"""``conduit`` command line: submit, worker, dlq, config."""

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
from conduit.core.queue import (
    SqsQueue,
    aws_client,
    ensure_queues,
    list_dead_letters,
    replay_dead_letters,
)
from conduit.models import Envelope, Task

app = typer.Typer(help="Sync tasks to Slack, Jira, and webhooks through one adapter interface.")
dlq_app = typer.Typer(help="Inspect and replay dead letters.")
config_app = typer.Typer(help="Validate and inspect connector YAML files.")
queues_app = typer.Typer(help="Create queues locally without Terraform.")
app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")
app.add_typer(queues_app, name="queues")

ConnectorsDir = Annotated[
    Path,
    typer.Option("--connectors-dir", envvar="CONDUIT_CONNECTORS_DIR", help="Directory of YAMLs"),
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
    repeat: Annotated[int, typer.Option(help="Submit each task this many times")] = 1,
) -> None:
    """Enqueue tasks for one connector; every task gets a deterministic idempotency key."""
    spec = _spec(connectors_dir, connector)
    tasks = _read_tasks(file)
    queue = SqsQueue.by_name(spec.queue_name)
    envelopes = (
        Envelope(
            task=t,
            connector=spec.name,
            idempotency_key=idempotency_key(spec.name, t.id, t.version),
        )
        for _ in range(repeat)
        for t in tasks
    )
    sent = queue.send_batch(envelopes)
    typer.echo(f"submitted {sent} messages ({len(tasks)} tasks x {repeat}) to {spec.queue_name}")


@app.command()
def worker(
    connector: Annotated[str, typer.Option("--connector", "-c")],
    connectors_dir: ConnectorsDir = Path("connectors"),
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
    if metrics_port > 0:
        metrics.serve(metrics_port)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    w = Worker(spec, adapter, store, queue)
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
    spec = _spec(connectors_dir, connector)
    moved = replay_dead_letters(
        SqsQueue.by_name(spec.dlq_name), SqsQueue.by_name(spec.queue_name), limit=limit
    )
    typer.echo(f"replayed {moved} messages from {spec.dlq_name} to {spec.queue_name}")


@config_app.command("validate")
def config_validate(connectors_dir: ConnectorsDir = Path("connectors")) -> None:
    """Parse every YAML and report each connector's type, target, and queue names."""
    try:
        specs = load_all(connectors_dir)
    except ConfigError as exc:
        typer.echo(f"invalid: {exc}", err=True)
        raise typer.Exit(2) from exc
    for spec in specs.values():
        typer.echo(
            f"ok  {spec.name:<16} {spec.type:<8} target={spec.target} "
            f"queue={spec.queue_name} dlq={spec.dlq_name} "
            f"max_receive={spec.queue.max_receive_count} attempts={spec.retry.max_attempts}"
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
        q, d = ensure_queues(spec, sqs)
        typer.echo(f"{spec.name}: {q.queue_url} -> {d.queue_url}")
    DynamoIdempotencyStore(table, client=aws_client("dynamodb")).ensure_table()
    typer.echo(f"table {table} ready")


if __name__ == "__main__":
    app()
