#!/usr/bin/env python3
"""Check that every SDK has a CI job, and which of them are required.

API2 Task 19 (docs/plans/2026-10-10-api2-unified-api-sdks.md) names two checks
on .github/workflows/ci.yml, both of which this script runs:

- every_sdk_dir_has_a_job: each language directory `sdks/<lang>` (all but
  conformance, fixtures and templates) has a job named `sdk-<lang>`, and
  each such job has a directory.
- only_wave1_is_required: every wave 1 job (D614: TypeScript, Python, Go,
  Rust) is in `required.needs`, and no wave 2 or 3 job is, except C++, C# and
  Swift. Those three were made required before API2 and API2-Q2 says they
  should not be; the plan's "Rulings made during execution" leaves that to
  the owner, and GRANDFATHERED goes when the owner rules.

It also checks that every wave 2 and 3 job reports a nightly failure as an
issue (Task 19: "A nightly failure opens or updates one issue per language").

The workflow is read line by line rather than with a YAML library so that the
job runs on a bare runner: it needs only the job keys (two-space indent under
`jobs:`), the `needs:` list of `required`, and the job bodies.

Usage:
    check-sdk-jobs.py [--root DIR] [--self-test]
"""
from pathlib import Path
import argparse
import re
import sys
import tempfile

NOT_LANGUAGES = {"conformance", "fixtures", "templates"}
WAVE1 = {"typescript", "python", "go", "rust"}
# Required before API2-Q2; see the module docstring.
GRANDFATHERED = {"cpp", "csharp", "swift"}
NIGHTLY_ISSUE_SCRIPT = "scripts/ci/sdk-nightly-issue.sh"
SELF_TEST_NOTE = "check-sdk-jobs: self-test passed"

JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):\s*(#.*)?$")


def read_jobs(workflow_text):
    """Return {job id: [body lines]} for the top-level `jobs:` mapping."""
    jobs = {}
    current = None
    in_jobs = False
    for line in workflow_text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            if current is not None:
                jobs[current].append(line)
            continue
        if not line.startswith(" "):
            in_jobs = line.rstrip() == "jobs:"
            current = None
            continue
        if not in_jobs:
            continue
        match = JOB_KEY.match(line)
        if match:
            current = match.group(1)
            jobs[current] = []
        elif current is not None:
            jobs[current].append(line)
    return jobs


def read_needs(body):
    """The block-list `needs:` of one job body (four-space indent)."""
    needs = []
    in_needs = False
    for line in body:
        if line.startswith("    needs:"):
            rest = line.split(":", 1)[1].strip()
            if rest:
                return [n.strip() for n in rest.strip("[]").split(",") if n.strip()]
            in_needs = True
            continue
        if in_needs:
            item = re.match(r"^      - ([A-Za-z0-9_-]+)\s*(#.*)?$", line)
            if item:
                needs.append(item.group(1))
            elif line.strip() and not line.lstrip().startswith("#"):
                break
    return needs


def check(root):
    """Return a list of failures for the tree at `root`."""
    failures = []
    workflow = root / ".github" / "workflows" / "ci.yml"
    jobs = read_jobs(workflow.read_text(encoding="utf-8"))
    sdk_dirs = {
        p.name
        for p in (root / "sdks").iterdir()
        if p.is_dir() and p.name not in NOT_LANGUAGES
    }
    sdk_jobs = {j[len("sdk-"):] for j in jobs if j.startswith("sdk-")}

    # every_sdk_dir_has_a_job
    for lang in sorted(sdk_dirs - sdk_jobs):
        failures.append(f"sdks/{lang} has no sdk-{lang} job in {workflow.name}")
    for lang in sorted(sdk_jobs - sdk_dirs):
        failures.append(f"job sdk-{lang} has no sdks/{lang} directory")

    # only_wave1_is_required
    if "required" not in jobs:
        failures.append("no `required` job")
        return failures
    required = {n[len("sdk-"):] for n in read_needs(jobs["required"]) if n.startswith("sdk-")}
    for lang in sorted(WAVE1 - required):
        failures.append(f"wave 1 job sdk-{lang} is not in required.needs")
    for lang in sorted(required - WAVE1 - GRANDFATHERED):
        failures.append(
            f"sdk-{lang} is in required.needs, but only wave 1 is required (API2-Q2)"
        )

    # A nightly failure of a wave 2 or 3 job opens or updates an issue.
    for lang in sorted(sdk_jobs - WAVE1):
        body = "\n".join(jobs[f"sdk-{lang}"])
        if NIGHTLY_ISSUE_SCRIPT not in body:
            failures.append(f"sdk-{lang} does not run {NIGHTLY_ISSUE_SCRIPT} on a nightly failure")
        elif "issues: write" not in body:
            failures.append(f"sdk-{lang} runs {NIGHTLY_ISSUE_SCRIPT} without `issues: write`")
    return failures


FIXTURE_WORKFLOW = """\
name: CI
on: [push]
jobs:
  changes:
    runs-on: ubuntu-24.04
  sdk-go:
    needs: changes
    runs-on: ubuntu-24.04
  sdk-ruby:  # a wave 3 job
    permissions:
      issues: write
    steps:
      - run: scripts/ci/sdk-nightly-issue.sh ruby
  required:
    needs:
      - changes
      # wave 1
      - sdk-go
    runs-on: ubuntu-24.04
"""


def self_test():
    def tree(dirs, workflow):
        tmp = Path(tempfile.mkdtemp(prefix="check-sdk-jobs-"))
        (tmp / ".github" / "workflows").mkdir(parents=True)
        (tmp / ".github" / "workflows" / "ci.yml").write_text(workflow, encoding="utf-8")
        for d in dirs:
            (tmp / "sdks" / d).mkdir(parents=True)
        return tmp

    def expect(name, failures, *needles):
        text = "\n".join(failures)
        if not needles and failures:
            raise AssertionError(f"{name}: expected no failures, got {failures}")
        for needle in needles:
            if needle not in text:
                raise AssertionError(f"{name}: expected {needle!r} in {failures}")

    global WAVE1
    saved = WAVE1
    WAVE1 = {"go"}
    try:
        base = ["go", "ruby", "conformance", "fixtures", "templates"]
        expect("clean", check(tree(base, FIXTURE_WORKFLOW)))
        expect("missing job", check(tree(base + ["php"], FIXTURE_WORKFLOW)),
               "sdks/php has no sdk-php job")
        expect("orphan job", check(tree(["go", "conformance"], FIXTURE_WORKFLOW)),
               "job sdk-ruby has no sdks/ruby directory")
        expect("wave 1 not required",
               check(tree(base, FIXTURE_WORKFLOW.replace("      - sdk-go\n", ""))),
               "wave 1 job sdk-go is not in required.needs")
        expect("wave 3 required",
               check(tree(base, FIXTURE_WORKFLOW.replace("- sdk-go\n", "- sdk-go\n      - sdk-ruby\n"))),
               "sdk-ruby is in required.needs")
        expect("grandfathered", check(tree(base + ["swift"], FIXTURE_WORKFLOW.replace(
            "  required:",
            "  sdk-swift:\n    permissions:\n      issues: write\n"
            "    steps:\n      - run: scripts/ci/sdk-nightly-issue.sh swift\n  required:",
        ).replace("- sdk-go\n", "- sdk-go\n      - sdk-swift\n"))))
        expect("no nightly issue",
               check(tree(base, FIXTURE_WORKFLOW.replace("scripts/ci/sdk-nightly-issue.sh ruby", "true"))),
               "sdk-ruby does not run")
        expect("no issues permission",
               check(tree(base, FIXTURE_WORKFLOW.replace("      issues: write\n", ""))),
               "without `issues: write`")
    finally:
        WAVE1 = saved
    print(SELF_TEST_NOTE)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    failures = check(args.root)
    for failure in failures:
        print(f"check-sdk-jobs: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("check-sdk-jobs: every SDK has a job; required.needs holds wave 1 (and C++, C#, Swift)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
