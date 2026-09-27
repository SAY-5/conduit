"""The fakes must behave like the real APIs closely enough for the adapters to pass."""

from __future__ import annotations

import importlib

import httpx
import pytest
from conduit.adapters import build_adapter
from conduit.core.retry import PermanentError, TransientError
from conduit.models import Task
from fastapi.testclient import TestClient


@pytest.fixture
def fake_clients(monkeypatch):
    modules = {
        name: importlib.reload(importlib.import_module(f"fakes.{name}"))
        for name in ("slack", "jira", "webhook")
    }
    return {name: TestClient(mod.app) for name, mod in modules.items()}


def adapter_for(specs, name, client: TestClient, base: str):
    spec = specs[name]
    update = {"target": f"{base}/hook"} if spec.type == "webhook" else {"base_url": base}
    spec = spec.model_copy(update=update)
    transport = httpx.MockTransport(
        lambda req: client.send(
            client.build_request(
                req.method, str(req.url), content=req.content, headers=dict(req.headers)
            )
        )
    )
    return build_adapter(spec, client=httpx.Client(transport=transport))


@pytest.mark.parametrize("name", ["slack-ops", "jira-support", "webhook-crm"])
def test_round_trip_and_inbox(specs, fake_clients, name):
    kind = specs[name].type
    client = fake_clients[kind]
    adapter = adapter_for(specs, name, client, "http://fake")
    task = Task(id="T-1", title="hello")
    result = adapter.deliver(task, "k" * 64)
    assert result.remote_id
    inbox = client.get("/_inbox").json()
    assert inbox["count"] == 1 and inbox["entries"][0]["idempotency_key"] == "k" * 64
    assert inbox["entries"][0]["task_id"] == "T-1"
    assert adapter.healthcheck()


@pytest.mark.parametrize("name", ["slack-ops", "jira-support", "webhook-crm"])
def test_fault_injection(specs, fake_clients, name):
    kind = specs[name].type
    client = fake_clients[kind]
    adapter = adapter_for(specs, name, client, "http://fake")
    client.post(
        "/_faults",
        json={
            "rate_limit_tasks": ["T-2"],
            "rate_limit_count": 1,
            "retry_after_seconds": 3,
            "hard_fail_tasks": ["T-3"],
        },
    )
    with pytest.raises(TransientError) as throttled:
        adapter.deliver(Task(id="T-2", title="x"), "a" * 64)
    assert throttled.value.status == 429 and throttled.value.retry_after == 3
    assert adapter.deliver(Task(id="T-2", title="x"), "a" * 64).remote_id
    with pytest.raises(PermanentError):
        adapter.deliver(Task(id="T-3", title="x"), "b" * 64)
    faults = client.get("/_faults").json()
    assert faults["rejected"] == 2 and faults["calls"] == 3
    client.delete("/_faults")
    assert adapter.deliver(Task(id="T-3", title="x"), "c" * 64).remote_id


@pytest.mark.parametrize("name", ["slack-ops", "jira-support", "webhook-crm"])
def test_outage_fails_every_call_until_cleared(specs, fake_clients, name):
    kind = specs[name].type
    client = fake_clients[kind]
    adapter = adapter_for(specs, name, client, "http://fake")
    client.post("/_faults", json={"outage": True})
    for suffix in ("a", "b"):
        with pytest.raises(TransientError) as raised:
            adapter.deliver(Task(id=f"T-{suffix}", title="x"), suffix * 64)
        assert raised.value.status == 503
    client.delete("/_faults")
    assert adapter.deliver(Task(id="T-a", title="x"), "a" * 64).remote_id


def test_webhook_fake_dedupes_and_verifies(specs, fake_clients):
    client = fake_clients["webhook"]
    adapter = adapter_for(specs, "webhook-crm", client, "http://fake")
    first = adapter.deliver(Task(id="T-9", title="x"), "d" * 64)
    second = adapter.deliver(Task(id="T-9", title="x"), "d" * 64)
    assert first.remote_id == second.remote_id
    assert client.get("/_inbox").json()["count"] == 1
    bad = client.post("/hook", content=b"{}", headers={"Idempotency-Key": "z"})
    assert bad.status_code == 401


def test_jira_fake_update_path(specs, fake_clients):
    client = fake_clients["jira"]
    adapter = adapter_for(specs, "jira-support", client, "http://fake")
    created = adapter.deliver(Task(id="T-5", title="one"), "e" * 64)
    updated = adapter.deliver(
        Task(id="T-5", version=2, title="two"), "f" * 64, remote_id=created.remote_id
    )
    assert created.remote_id == updated.remote_id
    ops = [e["op"] for e in client.get("/_inbox").json()["entries"]]
    assert ops == ["create", "update"]
    assert client.get("/_issues").json()["count"] == 1
    auth = {"Authorization": "Basic Ym90OnRva2Vu"}
    issue = client.get(f"/rest/api/3/issue/{created.remote_id}", headers=auth)
    assert issue.status_code == 200 and issue.json()["key"] == created.remote_id
    assert client.get(f"/rest/api/3/issue/{created.remote_id}").status_code == 401
    assert client.get("/rest/api/3/issue/SUP-999", headers=auth).status_code == 404
