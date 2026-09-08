from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class FaultSpec(BaseModel):
    """Failure injection, settable per fake at runtime or from the environment.

    ``rate_limit_tasks`` get HTTP 429 for their first ``rate_limit_count`` calls.
    ``hard_fail_tasks`` always get HTTP 400. ``fail_500_once`` returns one 500 on
    the first call of any task, then behaves.
    """

    rate_limit_tasks: list[str] = Field(default_factory=list)
    rate_limit_count: int = 2
    hard_fail_tasks: list[str] = Field(default_factory=list)
    fail_500_once: bool = False

    @classmethod
    def from_env(cls, prefix: str = "FAKE") -> FaultSpec:
        def split(name: str) -> list[str]:
            return [x for x in os.environ.get(f"{prefix}_{name}", "").split(",") if x]

        return cls(
            rate_limit_tasks=split("RATE_LIMIT_TASKS"),
            rate_limit_count=int(os.environ.get(f"{prefix}_RATE_LIMIT_COUNT", "2")),
            hard_fail_tasks=split("HARD_FAIL_TASKS"),
            fail_500_once=os.environ.get(f"{prefix}_500_ONCE", "").lower() in {"1", "true"},
        )


@dataclass
class FakeState:
    faults: FaultSpec = field(default_factory=FaultSpec.from_env)
    inbox: list[dict[str, Any]] = field(default_factory=list)
    rate_limit_hits: dict[str, int] = field(default_factory=dict)
    fired_500: set[str] = field(default_factory=set)
    calls: int = 0
    rejected: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def inject(self, task_id: str, error_body: dict[str, Any]) -> JSONResponse | None:
        """Return an error response when a fault applies to ``task_id``."""
        with self.lock:
            self.calls += 1
            f = self.faults
            if task_id in f.hard_fail_tasks:
                self.rejected += 1
                return JSONResponse({**error_body, "reason": "hard_fail"}, status_code=400)
            if task_id in f.rate_limit_tasks:
                hits = self.rate_limit_hits.get(task_id, 0)
                if hits < f.rate_limit_count:
                    self.rate_limit_hits[task_id] = hits + 1
                    self.rejected += 1
                    return JSONResponse(
                        {**error_body, "reason": "rate_limited"},
                        status_code=429,
                        headers={"Retry-After": "0"},
                    )
            if f.fail_500_once and task_id not in self.fired_500:
                self.fired_500.add(task_id)
                self.rejected += 1
                return JSONResponse({**error_body, "reason": "flaky"}, status_code=500)
        return None

    def record(self, **entry: Any) -> dict[str, Any]:
        entry.setdefault("received_at", time.time())
        with self.lock:
            entry["seq"] = len(self.inbox) + 1
            self.inbox.append(entry)
        return entry

    def seen_keys(self) -> set[str]:
        return {e["idempotency_key"] for e in self.inbox if e.get("idempotency_key")}


def admin_router(state: FakeState) -> APIRouter:
    router = APIRouter()

    @router.get("/_inbox")
    def inbox() -> dict[str, Any]:
        with state.lock:
            entries = list(state.inbox)
        keys = [e.get("idempotency_key") for e in entries]
        return {
            "count": len(entries),
            "unique_keys": len({k for k in keys if k}),
            "entries": entries,
        }

    @router.delete("/_inbox")
    def clear_inbox() -> dict[str, int]:
        with state.lock:
            n = len(state.inbox)
            state.inbox.clear()
        return {"cleared": n}

    @router.get("/_faults")
    def faults() -> dict[str, Any]:
        with state.lock:
            return {
                **state.faults.model_dump(),
                "calls": state.calls,
                "rejected": state.rejected,
                "rate_limit_hits": dict(state.rate_limit_hits),
            }

    @router.post("/_faults")
    def set_faults(spec: FaultSpec) -> dict[str, Any]:
        with state.lock:
            state.faults = spec
            state.rate_limit_hits.clear()
            state.fired_500.clear()
        return spec.model_dump()

    @router.delete("/_faults")
    def clear_faults() -> dict[str, Any]:
        with state.lock:
            state.faults = FaultSpec()
            state.rate_limit_hits.clear()
            state.fired_500.clear()
        return state.faults.model_dump()

    @router.get("/_health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return router


def make_app(title: str, state: FakeState) -> FastAPI:
    app = FastAPI(title=title, docs_url=None, redoc_url=None)
    app.include_router(admin_router(state))
    return app
