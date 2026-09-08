"""Fake webhook receiver: verifies the HMAC signature and dedupes on Idempotency-Key."""

from __future__ import annotations

import os
from typing import Any

from conduit.adapters.webhook import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify
from fastapi import Header, Request
from fastapi.responses import JSONResponse

from fakes.common import FakeState, make_app

state = FakeState()
app = make_app("fake-webhook", state)
SECRET = os.environ.get("FAKE_WEBHOOK_SECRET", "whsec-test")
delivered: dict[str, dict[str, Any]] = {}


@app.head("/hook")
def head_hook():
    return JSONResponse(None, status_code=200)


@app.post("/hook", status_code=202)
async def hook(
    request: Request,
    idempotency_key: str | None = Header(default=None),
    x_conduit_signature: str | None = Header(default=None, alias=SIGNATURE_HEADER),
    x_conduit_timestamp: str | None = Header(default=None, alias=TIMESTAMP_HEADER),
):
    body = await request.body()
    if not (x_conduit_signature and x_conduit_timestamp) or not verify(
        SECRET, x_conduit_timestamp, body, x_conduit_signature
    ):
        return JSONResponse({"error": "invalid signature"}, status_code=401)
    if not idempotency_key:
        return JSONResponse({"error": "Idempotency-Key required"}, status_code=400)
    payload = await request.json()
    task = payload.get("task", {})
    task_id = str(task.get("external_id") or task.get("id") or "")
    with state.lock:
        previous = delivered.get(idempotency_key)
    if previous is not None:
        return JSONResponse({**previous, "duplicate": True}, status_code=200)
    fault = state.inject(task_id, {"error": "injected"})
    if fault is not None:
        return fault
    entry = state.record(
        task_id=task_id, idempotency_key=idempotency_key, event=payload.get("event")
    )
    result = {"id": f"crm-{entry['seq']}", "task_id": task_id}
    with state.lock:
        delivered[idempotency_key] = result
    return result
