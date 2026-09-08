"""LocalStack-backed fixtures. Set CONDUIT_LOCALSTACK_URL (make up) or these tests skip."""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import boto3
import httpx
import pytest
import uvicorn
from conduit.adapters import build_adapter
from conduit.config import ConnectorSpec
from conduit.core.idempotency import DynamoIdempotencyStore, idempotency_key
from conduit.core.queue import SqsQueue, ensure_queues
from conduit.models import Envelope, Task
from conduit.worker import Worker

LOCALSTACK = os.environ.get("CONDUIT_LOCALSTACK_URL")

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(items):
    if LOCALSTACK:
        return
    skip = pytest.mark.skip(reason="CONDUIT_LOCALSTACK_URL not set")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class FakeServer:
    kind: str
    base_url: str
    client: httpx.Client

    def faults(self, **spec) -> None:
        self.client.post("/_faults", json=spec).raise_for_status()

    def clear_faults(self) -> None:
        self.client.delete("/_faults").raise_for_status()

    def inbox(self) -> dict:
        return self.client.get("/_inbox").json()


@pytest.fixture(scope="session")
def fakes() -> Iterator[dict[str, FakeServer]]:
    servers: dict[str, FakeServer] = {}
    threads = []
    for kind in ("slack", "jira", "webhook"):
        port = _free_port()
        config = uvicorn.Config(
            f"fakes.{kind}:app", host="127.0.0.1", port=port, log_level="warning"
        )
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        threads.append((server, thread))
        base = f"http://127.0.0.1:{port}"
        client = httpx.Client(base_url=base, timeout=5)
        for _ in range(100):
            try:
                if client.get("/_health").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.05)
        servers[kind] = FakeServer(kind, base, client)
    yield servers
    for server, thread in threads:
        server.should_exit = True
        thread.join(timeout=5)


@pytest.fixture(scope="session")
def aws():
    return {
        "sqs": boto3.client("sqs", endpoint_url=LOCALSTACK, region_name="us-east-1"),
        "dynamodb": boto3.client("dynamodb", endpoint_url=LOCALSTACK, region_name="us-east-1"),
    }


@pytest.fixture(scope="session")
def store(aws) -> DynamoIdempotencyStore:
    store = DynamoIdempotencyStore(
        "conduit-it-idempotency", client=aws["dynamodb"], lease_seconds=20
    )
    store.ensure_table()
    return store


@dataclass
class Rig:
    spec: ConnectorSpec
    queue: SqsQueue
    dlq: SqsQueue
    fake: FakeServer
    store: DynamoIdempotencyStore
    worker: Worker

    def submit(self, *tasks: Task) -> None:
        self.queue.send_batch(
            Envelope(
                task=t,
                connector=self.spec.name,
                idempotency_key=idempotency_key(self.spec.name, t.id, t.version),
            )
            for t in tasks
        )

    def run(self, idle_polls: int = 2):
        return self.worker.run(idle_polls=idle_polls, wait_seconds=1)


@pytest.fixture
def rig_factory(specs, fakes, aws, store, monkeypatch):
    """Build an isolated queue pair + worker for a shipped connector, pointed at a fake."""

    def make(name: str, **overrides) -> Rig:
        base = specs[name]
        fake = fakes[base.type]
        suffix = uuid.uuid4().hex[:8]
        update = {"name": f"it-{base.type}-{suffix}"}
        if base.type == "webhook":
            update["target"] = f"{fake.base_url}/hook"
        else:
            update["base_url"] = fake.base_url
        update.update(overrides)
        spec = base.model_copy(update=update)
        queue, dlq = ensure_queues(spec, aws["sqs"])
        adapter = build_adapter(spec)
        worker = Worker(spec, adapter, store, queue)
        fake.clear_faults()
        fake.client.delete("/_inbox")
        return Rig(spec, queue, dlq, fake, store, worker)

    return make


def task(task_id: str, version: int = 1, **kw) -> Task:
    kw.setdefault("title", f"task {task_id}")
    return Task(id=task_id, version=version, **kw)
