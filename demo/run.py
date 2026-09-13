"""End-to-end demo against LocalStack and the fakes; prints a summary with measured numbers.

Run through ``make demo`` (compose up + terraform apply + this script). Every
figure below is read back from the queues, the fakes' inboxes, the workers'
logs and metrics, and a real ``terraform plan``.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path

import boto3
import httpx
from conduit.config import load_all
from conduit.core.idempotency import idempotency_key
from conduit.core.queue import SqsQueue, list_dead_letters, replay_dead_letters
from conduit.models import Envelope, Task
from conduit.worker import percentile

ROOT = Path(__file__).resolve().parent.parent
LOCALSTACK = os.environ.get("CONDUIT_LOCALSTACK_URL", "http://localhost:4566")
FAKES = {
    "slack": "http://localhost:8001",
    "jira": "http://localhost:8002",
    "webhook": "http://localhost:8003",
}
METRICS_PORT = {"slack-ops": 9101, "jira-support": 9102, "webhook-crm": 9103}
COMPOSE = ["docker", "compose", "-f", str(ROOT / "deploy" / "docker-compose.yml")]

UNIQUE_PER_CONNECTOR = 80
DUPLICATES_PER_CONNECTOR = 20
JIRA_RATE_LIMITED_TASKS = 30
JIRA_429_ATTEMPTS = 2
WEBHOOK_HARD_FAIL_TASKS = 10
DRAIN_TIMEOUT = 300

NEW_CONNECTOR_YAML = """\
type: slack
target: "#oncall"
secrets:
  token: PAGER_SLACK_TOKEN
retry:
  max_attempts: 6
queue:
  max_receive_count: 4
"""


def say(msg: str) -> None:
    print(f"[demo] {msg}", flush=True)


def wait_http(url: str, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=2).status_code < 500:
                return
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise SystemExit(f"timeout waiting for {url}")


def scrape(connector: str) -> dict[str, float]:
    text = httpx.get(f"http://localhost:{METRICS_PORT[connector]}/metrics", timeout=5).text
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("conduit_") and f'connector="{connector}"' in line and "_total" in line:
            name, value = line.rsplit(" ", 1)
            out[name.split("{")[0]] = float(value)
    return out


def worker_logs(service: str, run_id: str) -> list[dict]:
    raw = subprocess.run(
        [*COMPOSE, "logs", "--no-log-prefix", "--no-color", service],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    events = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if str(event.get("task_id", "")).startswith(run_id):
            events.append(event)
    return events


def queue_totals(queue: SqsQueue) -> int:
    return sum(queue.depth().values())


def wait_for_drain(queues: dict[str, SqsQueue], timeout: float) -> float:
    started = time.monotonic()
    stable = 0
    while time.monotonic() - started < timeout:
        remaining = {name: queue_totals(q) for name, q in queues.items()}
        if all(v == 0 for v in remaining.values()):
            stable += 1
            if stable >= 3:
                return time.monotonic() - started
        else:
            stable = 0
        time.sleep(1)
    raise SystemExit(f"queues did not drain within {timeout}s: {remaining}")


def terraform_plan_for_new_yaml() -> tuple[str, list[str]]:
    with tempfile.TemporaryDirectory() as tmp:
        connectors = Path(tmp) / "connectors"
        shutil.copytree(ROOT / "connectors", connectors)
        (connectors / "pager-oncall.yaml").write_text(NEW_CONNECTOR_YAML)
        result = subprocess.run(
            [
                "terraform",
                f"-chdir={ROOT / 'terraform'}",
                "plan",
                "-input=false",
                "-no-color",
                "-var-file=localstack.tfvars",
                f"-var=connectors_dir={connectors}",
                "-state=localstack.tfstate",
                "-lock=false",
            ],
            capture_output=True,
            text=True,
            env={**os.environ, "TF_IN_AUTOMATION": "1"},
            check=False,
        )
    if result.returncode != 0:
        raise SystemExit(f"terraform plan failed:\n{result.stderr}")
    summary = next(
        (ln.strip() for ln in result.stdout.splitlines() if ln.strip().startswith("Plan:")), ""
    )
    created = [
        ln.strip().removeprefix("# ").removesuffix(" will be created")
        for ln in result.stdout.splitlines()
        if ln.strip().startswith("# ") and ln.strip().endswith("will be created")
    ]
    return summary, created


def main() -> int:
    run_id = f"D{uuid.uuid4().hex[:6].upper()}"
    rng = random.Random(run_id)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    sqs = boto3.client("sqs", endpoint_url=LOCALSTACK, region_name="us-east-1")

    specs = load_all(ROOT / "connectors")
    say(f"run {run_id}: waiting for fakes, workers, and Terraform-created queues")
    for base in FAKES.values():
        wait_http(f"{base}/_health")
    for port in METRICS_PORT.values():
        wait_http(f"http://localhost:{port}/metrics", timeout=240)
    queues = {name: SqsQueue.by_name(spec.queue_name, sqs) for name, spec in specs.items()}
    dlqs = {name: SqsQueue.by_name(spec.dlq_name, sqs) for name, spec in specs.items()}
    fakes = {
        spec.type: httpx.Client(base_url=FAKES[spec.type], timeout=10) for spec in specs.values()
    }

    for client in fakes.values():
        client.delete("/_inbox")
        client.delete("/_faults")
    before = {name: scrape(name) for name in specs}

    # Synthetic tasks: 80 unique per connector, 20 resubmits per connector, interleaved.
    tasks: dict[str, list[Task]] = {}
    for name in specs:
        tasks[name] = [
            Task(
                id=f"{run_id}-{name}-{i:04d}",
                title=f"Synthetic task {i} for {name}",
                body=f"Body of task {i}",
                priority=rng.choice(["low", "normal", "high"]),
                labels=[name.split("-")[0], "demo"],
            )
            for i in range(1, UNIQUE_PER_CONNECTOR + 1)
        ]
    jira_tasks = tasks["jira-support"]
    webhook_tasks = tasks["webhook-crm"]
    rate_limited = [t.id for t in jira_tasks[:JIRA_RATE_LIMITED_TASKS]]
    hard_failed = [t.id for t in webhook_tasks[:WEBHOOK_HARD_FAIL_TASKS]]
    fakes["jira"].post(
        "/_faults", json={"rate_limit_tasks": rate_limited, "rate_limit_count": JIRA_429_ATTEMPTS}
    ).raise_for_status()
    fakes["webhook"].post("/_faults", json={"hard_fail_tasks": hard_failed}).raise_for_status()
    say(
        f"faults: jira 429 x{JIRA_429_ATTEMPTS} for {len(rate_limited)} tasks, "
        f"webhook 400 for {len(hard_failed)} tasks"
    )

    submitted = 0
    duplicates = 0
    submit_started = time.time()
    for name, spec in specs.items():
        unique = tasks[name]
        healthy = [t for t in unique if t.id not in hard_failed]
        resubmits = rng.sample(healthy, DUPLICATES_PER_CONNECTOR)
        batch = unique + resubmits
        rng.shuffle(batch)
        envelopes = [
            Envelope(task=t, connector=name, idempotency_key=idempotency_key(name, t.id, t.version))
            for t in batch
        ]
        sent = queues[name].send_batch(envelopes)
        submitted += sent
        duplicates += len(resubmits)
        say(
            f"submitted {sent} messages to {spec.queue_name} ({len(unique)} unique, {len(resubmits)} resubmits)"
        )

    drain_seconds = wait_for_drain(queues, DRAIN_TIMEOUT)
    say(f"queues drained in {drain_seconds:.1f}s")
    after = {name: scrape(name) for name in specs}
    delta = {
        name: {k: after[name].get(k, 0) - before[name].get(k, 0) for k in after[name]}
        for name in specs
    }

    inbox = {name: fakes[spec.type].get("/_inbox").json() for name, spec in specs.items()}
    delivered = {
        name: sum(1 for e in inbox[name]["entries"] if str(e["task_id"]).startswith(run_id))
        for name in specs
    }
    unique_keys = {
        name: len(
            {
                e["idempotency_key"]
                for e in inbox[name]["entries"]
                if str(e["task_id"]).startswith(run_id)
            }
        )
        for name in specs
    }
    dead = {name: list_dead_letters(dlqs[name]) for name in specs}
    dead_ids = {
        name: sorted(
            m.envelope.task.id for m in dead[name] if m.envelope.task.id.startswith(run_id)
        )
        for name in specs
    }

    logs = {name: worker_logs(f"worker-{name}", run_id) for name in specs}
    retries = {
        name: [e for e in logs[name] if e.get("event") == "delivery.retry"] for name in specs
    }
    latencies = {
        name: [float(e["elapsed"]) for e in logs[name] if e.get("event") == "delivery.ok"]
        for name in specs
    }
    dedup_logged = {
        name: sum(1 for e in logs[name] if e.get("event") == "delivery.deduplicated")
        for name in specs
    }
    end_to_end = {
        name: [
            float(e["received_at"]) - submit_started
            for e in inbox[name]["entries"]
            if str(e["task_id"]).startswith(run_id)
        ]
        for name in specs
    }

    jira_faults = fakes["jira"].get("/_faults").json()
    say("clearing the webhook fault and replaying the dead letters")
    fakes["webhook"].delete("/_faults").raise_for_status()
    replayed = replay_dead_letters(dlqs["webhook-crm"], queues["webhook-crm"])
    replay_seconds = wait_for_drain({"webhook-crm": queues["webhook-crm"]}, 120)
    dlq_after_replay = len(list_dead_letters(dlqs["webhook-crm"]))
    webhook_after = fakes["webhook"].get("/_inbox").json()
    webhook_delivered_after = sum(
        1 for e in webhook_after["entries"] if str(e["task_id"]).startswith(run_id)
    )
    replay_metrics = scrape("webhook-crm")

    say("terraform plan with a fourth connector YAML")
    plan_summary, plan_created = terraform_plan_for_new_yaml()

    total_unique = sum(len(v) for v in tasks.values())
    total_dedup = int(sum(delta[n].get("conduit_deduplicated_total", 0) for n in specs))
    total_retried = int(sum(delta[n].get("conduit_retried_total", 0) for n in specs))
    total_dead = sum(len(v) for v in dead_ids.values())
    all_delays = [float(e["delay"]) for n in specs for e in retries[n]]
    all_latency = [x for n in specs for x in latencies[n]]
    all_e2e = [x for n in specs for x in end_to_end[n]]
    by_attempt = Counter(int(e["attempt"]) for e in retries["jira-support"])

    lines = [
        "",
        f"conduit demo summary (run {run_id}, LocalStack at {LOCALSTACK})",
        "=" * 72,
        f"tasks submitted        {submitted}  ({total_unique} unique + {duplicates} duplicate resubmits)",
        f"deduplicated           {total_dedup}  (must equal duplicates: {'ok' if total_dedup == duplicates else 'MISMATCH'})",
        "delivered per connector",
    ]
    for name, spec in specs.items():
        lines.append(
            f"  {name:<14} {delivered[name]:>4} delivered, {unique_keys[name]:>3} unique keys, "
            f"{int(delta[name].get('conduit_deduplicated_total', 0)):>3} deduplicated "
            f"({spec.type} fake inbox)"
        )
    lines += [
        f"retried                {total_retried}  (jira fake returned 429 {jira_faults['rejected']} times "
        f"for {len(jira_faults['rate_limit_hits'])} tasks)",
        f"  backoff evidence     attempt 1 -> {by_attempt.get(1, 0)} retries, attempt 2 -> {by_attempt.get(2, 0)} retries; "
        f"delay min/median/max {min(all_delays):.3f}s / {percentile(all_delays, 50):.3f}s / {max(all_delays):.3f}s "
        f"(policy base 0.25s x2, cap 8s, full jitter)",
        f"dead-lettered          {total_dead}  (must equal hard failures {WEBHOOK_HARD_FAIL_TASKS}: "
        f"{'ok' if total_dead == WEBHOOK_HARD_FAIL_TASKS else 'MISMATCH'}); "
        f"conduit-webhook-crm-dlq after maxReceiveCount={specs['webhook-crm'].queue.max_receive_count}",
        f"  dead letters         {', '.join(t.rsplit('-', 1)[-1] for t in dead_ids['webhook-crm'])}",
        f"DLQ replay             {replayed} replayed after clearing the fault; DLQ now {dlq_after_replay}; "
        f"webhook delivered {webhook_delivered_after}/{UNIQUE_PER_CONNECTOR} in {replay_seconds:.1f}s",
        f"delivery latency       p50 {percentile(all_latency, 50) * 1000:.1f} ms, p95 {percentile(all_latency, 95) * 1000:.1f} ms "
        f"(worker attempt-to-ack, n={len(all_latency)})",
        f"end-to-end latency     p50 {percentile(all_e2e, 50):.2f} s, p95 {percentile(all_e2e, 95):.2f} s "
        f"(submit-to-remote-receipt, n={len(all_e2e)}); queues drained in {drain_seconds:.1f}s",
        "new integration from one file (connectors/pager-oncall.yaml, 10 lines):",
        f"  {plan_summary}",
    ]
    lines += [f"  + {addr}" for addr in plan_created]
    lines.append("=" * 72)
    report = "\n".join(lines)
    print(report, flush=True)
    out = ROOT / "demo" / "out"
    out.mkdir(exist_ok=True)
    (out / "summary.txt").write_text(report + "\n")
    (out / "details.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "metrics_delta": delta,
                "replay_metrics": replay_metrics,
                "dedup_logged": dedup_logged,
                "dead_letters": dead_ids,
                "plan": {"summary": plan_summary, "created": plan_created},
            },
            indent=2,
            default=str,
        )
    )

    problems = []
    if total_dedup != duplicates:
        problems.append(f"deduplicated {total_dedup} != duplicates {duplicates}")
    if total_dead != WEBHOOK_HARD_FAIL_TASKS:
        problems.append(f"dead letters {total_dead} != hard failures {WEBHOOK_HARD_FAIL_TASKS}")
    if dlq_after_replay != 0 or webhook_delivered_after != UNIQUE_PER_CONNECTOR:
        problems.append("replay did not drain the DLQ")
    if any(delivered[n] != UNIQUE_PER_CONNECTOR for n in ("slack-ops", "jira-support")):
        problems.append(f"delivered {delivered}")
    if len(plan_created) != 8 or "8 to add" not in plan_summary:
        problems.append(f"unexpected plan: {plan_summary}")
    if problems:
        print("DEMO CHECKS FAILED: " + "; ".join(problems), file=sys.stderr)
        return 1
    say("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
