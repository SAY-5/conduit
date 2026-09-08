"""Fake Slack Web API: chat.postMessage and auth.test."""

from __future__ import annotations

from typing import Any

from fastapi import Header, Request
from fastapi.responses import JSONResponse

from fakes.common import FakeState, make_app

state = FakeState()
app = make_app("fake-slack", state)


@app.post("/api/auth.test")
def auth_test(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        return {"ok": False, "error": "not_authed"}
    return {"ok": True, "user": "conduit", "team": "fake"}


@app.post("/api/chat.postMessage")
async def post_message(
    request: Request,
    authorization: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
):
    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse({"ok": False, "error": "not_authed"}, status_code=200)
    body = await request.json()
    payload = body.get("metadata", {}).get("event_payload", {})
    task_id = str(payload.get("task_id", ""))
    key = payload.get("idempotency_key") or idempotency_key
    if not body.get("channel") or not body.get("blocks"):
        return JSONResponse({"ok": False, "error": "invalid_blocks"}, status_code=200)
    fault = state.inject(task_id, {"ok": False, "error": "ratelimited"})
    if fault is not None:
        if fault.status_code == 400:
            return JSONResponse({"ok": False, "error": "invalid_blocks"}, status_code=200)
        return fault
    entry = state.record(
        task_id=task_id,
        idempotency_key=key,
        channel=body["channel"],
        text=body.get("text"),
        version=payload.get("version"),
    )
    return {"ok": True, "channel": body["channel"], "ts": f"{entry['received_at']:.6f}"}
