"""Fake Jira Cloud REST v3: create, update, get issue, and myself."""

from __future__ import annotations

import re
from typing import Any

from fastapi import Header, Request
from fastapi.responses import JSONResponse

from fakes.common import FakeState, make_app

state = FakeState()
app = make_app("fake-jira", state)
issues: dict[str, dict[str, Any]] = {}
counter = {"n": 0}
TAG = re.compile(r"\[conduit:([^\]]+)\]")


def _task_id(fields: dict[str, Any]) -> str:
    match = TAG.search(str(fields.get("summary", "")))
    if match:
        return match.group(1)
    return str(fields.get("customfield_10042", ""))


def _unauthorized(authorization: str | None) -> JSONResponse | None:
    if not authorization or not authorization.startswith("Basic "):
        return JSONResponse({"errorMessages": ["Unauthorized"]}, status_code=401)
    return None


@app.get("/rest/api/3/myself")
def myself(authorization: str | None = Header(default=None)):
    return _unauthorized(authorization) or {"accountId": "fake", "displayName": "Conduit"}


@app.post("/rest/api/3/issue", status_code=201)
async def create_issue(
    request: Request,
    authorization: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
):
    if denied := _unauthorized(authorization):
        return denied
    fields = (await request.json()).get("fields", {})
    if not fields.get("project", {}).get("key") or not fields.get("summary"):
        return JSONResponse({"errors": {"summary": "required"}}, status_code=400)
    task_id = _task_id(fields)
    fault = state.inject(task_id, {"errorMessages": ["injected"]})
    if fault is not None:
        return fault
    with state.lock:
        counter["n"] += 1
        key = f"{fields['project']['key']}-{counter['n']}"
        issues[key] = {"key": key, "fields": fields, "versions": 1}
    state.record(task_id=task_id, idempotency_key=idempotency_key, issue=key, op="create")
    return {"id": str(10000 + counter["n"]), "key": key, "self": f"/rest/api/3/issue/{key}"}


@app.put("/rest/api/3/issue/{key}", status_code=204)
async def update_issue(
    key: str,
    request: Request,
    authorization: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None),
):
    if denied := _unauthorized(authorization):
        return denied
    if key not in issues:
        return JSONResponse({"errorMessages": ["Issue does not exist"]}, status_code=404)
    fields = (await request.json()).get("fields", {})
    task_id = _task_id(fields) or _task_id(issues[key]["fields"])
    fault = state.inject(task_id, {"errorMessages": ["injected"]})
    if fault is not None:
        return fault
    with state.lock:
        issues[key]["fields"].update(fields)
        issues[key]["versions"] += 1
    state.record(task_id=task_id, idempotency_key=idempotency_key, issue=key, op="update")
    return JSONResponse(None, status_code=204)


@app.get("/rest/api/3/issue/{key}")
def get_issue(key: str, authorization: str | None = Header(default=None)):
    if denied := _unauthorized(authorization):
        return denied
    if key not in issues:
        return JSONResponse({"errorMessages": ["Issue does not exist"]}, status_code=404)
    return issues[key]


@app.get("/_issues")
def list_issues() -> dict[str, Any]:
    return {"count": len(issues), "issues": list(issues.values())}
