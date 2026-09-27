"""Check the demo block pasted into README.md against the repository it claims to measure.

The block's second line records the commit, package version, LocalStack image, and date of the
run that produced it. This script asserts that provenance is real and internally consistent, and
lists the commits that have touched the measured code since, so a stale paste is visible. Run it
through ``make demo-verify`` after ``make demo``, or on its own in CI.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import conduit

ROOT = Path(__file__).resolve().parent.parent
MEASURED = re.compile(
    r"^measured at commit (?P<commit>[0-9a-f]{7,40}), conduit (?P<version>[^,]+), "
    r"(?P<image>\S+), (?P<date>\d{4}-\d{2}-\d{2})$",
    re.MULTILINE,
)
WATCHED = ("conduit/", "demo/", "fakes/", "connectors/", "schemas/", "terraform/")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=False
    ).stdout.strip()


def main() -> int:
    readme = (ROOT / "README.md").read_text()
    match = MEASURED.search(readme)
    problems: list[str] = []
    if match is None:
        print("README.md has no 'measured at commit ...' line", file=sys.stderr)
        return 1

    commit, version, image = match["commit"], match["version"], match["image"]
    print(f"demo block measured at {commit}, conduit {version}, {image}, {match['date']}")

    if git("cat-file", "-t", commit) != "commit":
        problems.append(f"commit {commit} is not in this repository")
    elif subprocess.run(
        ["git", "-C", str(ROOT), "merge-base", "--is-ancestor", commit, "HEAD"], check=False
    ).returncode:
        problems.append(f"commit {commit} is not an ancestor of HEAD")

    if version != conduit.__version__:
        problems.append(f"block says conduit {version}, package is {conduit.__version__}")

    compose = (ROOT / "deploy" / "docker-compose.yml").read_text()
    shipped = next(
        (ln.split("image:", 1)[1].strip() for ln in compose.splitlines() if "localstack/" in ln), ""
    )
    if image != shipped:
        problems.append(f"block says {image}, docker-compose.yml runs {shipped}")

    start = readme.index("conduit demo summary")
    block = readme[start : readme.index("```", start)]
    if "MISMATCH" in block:
        problems.append("the pasted block reports a MISMATCH")
    checks = re.findall(r"\(must equal [^)]*: (ok|MISMATCH)\)", block)
    if not checks:
        problems.append("the pasted block carries no 'must equal' checks")
    if checks and all(c == "ok" for c in checks):
        print(f"{len(checks)} self-checks in the block, all ok")

    since = [
        line
        for line in git("log", "--oneline", f"{commit}..HEAD", "--", *WATCHED).splitlines()
        if line
    ]
    if since:
        print(f"{len(since)} commit(s) have touched the measured code since {commit}:")
        for line in since:
            print(f"  {line}")
        print("re-run make demo and repaste the block before tagging a release")

    for problem in problems:
        print(f"README CHECK FAILED: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
