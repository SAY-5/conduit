"""What an operator asks at 3am: how deep are the queues and is anything stuck.

Queue depths come straight from SQS. Worker state comes from a status row each
worker writes into the idempotency table every few seconds, so the summary needs
no knowledge of where the workers run or which port they serve metrics on. A row
carries the run it belongs to, which is what makes the cost report per run
rather than cumulative.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

from conduit.config import ConnectorSpec
from conduit.core.queue import SqsQueue, queue_exists

STATUS_TTL_SECONDS = 3600
# us-east-1 on-demand list prices, quoted per million so the arithmetic below
# stays in whole units. Requests are what a pipeline like this actually buys.
PRICE_PER_MILLION = {
    "sqs_requests": 0.40,
    "dynamodb_writes": 1.25,
    "dynamodb_reads": 0.25,
}


def status_pk(connector: str) -> str:
    return f"status|{connector}"


@dataclass
class WorkerStatus:
    """The snapshot one worker publishes about its current run."""

    connector: str
    run_id: str
    started_at: float
    updated_at: float
    received: int = 0
    delivered: int = 0
    deduplicated: int = 0
    quarantined: int = 0
    retried: int = 0
    failed: int = 0
    dead_lettered: int = 0
    sqs_requests: int = 0
    dynamodb_writes: int = 0
    dynamodb_reads: int = 0
    remote_objects: int = 0
    breaker_state: str = "closed"
    tokens_available: float = 0.0
    rate_limit_waits: int = 0
    last_error: str = ""
    last_error_at: float = 0.0

    @property
    def run_seconds(self) -> float:
        return max(0.0, self.updated_at - self.started_at)

    def per_minute(self) -> float:
        """Delivered per minute over this run, 0 until the run has any length."""
        if self.run_seconds <= 0:
            return 0.0
        return self.delivered / self.run_seconds * 60


@dataclass
class Depth:
    visible: int = 0
    in_flight: int = 0

    @property
    def total(self) -> int:
        return self.visible + self.in_flight


@dataclass
class ConnectorOps:
    name: str
    type: str
    work: Depth = field(default_factory=Depth)
    dlq: Depth = field(default_factory=Depth)
    quarantine: Depth = field(default_factory=Depth)
    status: WorkerStatus | None = None


class StatusStore:
    """Worker status rows, kept in the idempotency table under a ``status|`` key."""

    def __init__(self, table_name: str, *, client: Any, clock=time.time) -> None:
        self.table_name = table_name
        self._client = client
        self._clock = clock

    def publish(self, status: WorkerStatus) -> None:
        item: dict[str, Any] = {
            "pk": {"S": status_pk(status.connector)},
            "expires_at": {"N": str(int(self._clock()) + STATUS_TTL_SECONDS)},
        }
        for key, value in asdict(status).items():
            item[key] = {"S": value} if isinstance(value, str) else {"N": repr(float(value))}
        self._client.put_item(TableName=self.table_name, Item=item)

    def read(self, connector: str) -> WorkerStatus | None:
        item = self._client.get_item(
            TableName=self.table_name,
            Key={"pk": {"S": status_pk(connector)}},
            ConsistentRead=True,
        ).get("Item")
        if not item:
            return None
        fields: dict[str, Any] = {}
        for name, kind in WorkerStatus.__annotations__.items():
            raw = item.get(name)
            if raw is None:
                continue
            if "str" in kind:
                fields[name] = raw.get("S", "")
            elif "float" in kind:
                fields[name] = float(raw.get("N", 0))
            else:
                fields[name] = int(float(raw.get("N", 0)))
        return WorkerStatus(**fields)


def depth_of(queue: SqsQueue) -> Depth:
    attrs = queue.depth()
    return Depth(
        visible=attrs.get("ApproximateNumberOfMessages", 0),
        in_flight=attrs.get("ApproximateNumberOfMessagesNotVisible", 0),
    )


def collect(specs: dict[str, ConnectorSpec], sqs: Any, statuses: StatusStore) -> list[ConnectorOps]:
    """Read live depths for every connector plus whatever its worker last published."""
    rows = []
    for name, spec in sorted(specs.items()):
        row = ConnectorOps(name=name, type=spec.type, status=statuses.read(name))
        for attr, queue_name in (
            ("work", spec.queue_name),
            ("dlq", spec.dlq_name),
            ("quarantine", spec.quarantine_name),
        ):
            if queue_exists(queue_name, sqs):
                setattr(row, attr, depth_of(SqsQueue.by_name(queue_name, sqs)))
        rows.append(row)
    return rows


def render_summary(rows: list[ConnectorOps]) -> str:
    """One fixed-width line per connector plus a totals line."""
    header = (
        f"{'connector':<16}{'queue':>7}{'inflight':>9}{'dlq':>6}{'quar':>6}"
        f"{'delivered':>11}{'per min':>9}{'throttle':>10}{'breaker':>9}  last error"
    )
    lines = [header, "-" * len(header)]
    totals = Depth()
    delivered = 0
    for row in rows:
        s = row.status
        totals.visible += row.work.visible + row.dlq.visible + row.quarantine.visible
        totals.in_flight += row.work.in_flight + row.dlq.in_flight + row.quarantine.in_flight
        delivered += s.delivered if s else 0
        lines.append(
            f"{row.name:<16}{row.work.visible:>7}{row.work.in_flight:>9}"
            f"{row.dlq.total:>6}{row.quarantine.total:>6}"
            f"{(s.delivered if s else 0):>11}{(s.per_minute() if s else 0.0):>9.1f}"
            f"{(f'{s.tokens_available:.1f} tok' if s else '-'):>10}"
            f"{(s.breaker_state if s else '-'):>9}  {(s.last_error if s else 'no worker seen')}"
        )
    lines.append("-" * len(header))
    lines.append(
        f"{'total':<16}{totals.visible:>7}{totals.in_flight:>9}{'':>6}{'':>6}{delivered:>11}"
    )
    return "\n".join(lines)


def render_costs(rows: list[ConnectorOps]) -> str:
    """Counts written this run, priced at the documented per-million rates."""
    header = (
        f"{'connector':<16}{'run':>10}{'objects':>9}{'sqs req':>9}"
        f"{'ddb write':>11}{'ddb read':>10}{'usd':>10}"
    )
    lines = [header, "-" * len(header)]
    counted = ("remote_objects", "sqs_requests", "dynamodb_writes", "dynamodb_reads")
    totals = dict.fromkeys(counted, 0)
    for row in rows:
        s = row.status
        if s is None:
            lines.append(f"{row.name:<16}{'no worker seen':>10}")
            continue
        counts = {k: getattr(s, k) for k in totals}
        for key, value in counts.items():
            totals[key] += value
        lines.append(
            f"{row.name:<16}{s.run_id:>10}{counts['remote_objects']:>9}"
            f"{counts['sqs_requests']:>9}{counts['dynamodb_writes']:>11}"
            f"{counts['dynamodb_reads']:>10}{usd(counts):>10.4f}"
        )
    lines.append("-" * len(header))
    lines.append(
        f"{'total':<16}{'':>10}{totals['remote_objects']:>9}{totals['sqs_requests']:>9}"
        f"{totals['dynamodb_writes']:>11}{totals['dynamodb_reads']:>10}{usd(totals):>10.4f}"
    )
    rates = ", ".join(f"{k} ${v}/M" for k, v in PRICE_PER_MILLION.items())
    lines.append(f"priced at us-east-1 on-demand list: {rates}")
    return "\n".join(lines)


def usd(counts: dict[str, int]) -> float:
    return sum(counts.get(key, 0) / 1_000_000 * price for key, price in PRICE_PER_MILLION.items())
