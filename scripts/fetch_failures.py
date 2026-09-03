#!/usr/bin/env python3
"""
Fetch GitHub Actions job/step results from a monitored repository,
extract configuration failures, classify them, write structured JSON
output, and push each record into Elasticsearch.

Supports two modes:
  - Polling mode (default): scans the most recent N runs
  - Event-driven mode (--run-id): targets one specific run,
    used when triggered by the webhook listener via repository_dispatch
"""

import os
import re
import sys
import json
import argparse
from datetime import datetime, timezone

import requests

GITHUB_API = "https://api.github.com"

FAILURE_PATTERNS = [
    (r"npm error code E404|npm error.*404 Not Found", "dependency_install_failure"),
    (r"ENOTFOUND|ETIMEDOUT|ECONNREFUSED", "network_proxy_failure"),
    (r"No such file or directory", "workspace_path_failure"),
    (r"Permission denied", "permission_failure"),
    (r"No space left on device", "disk_space_failure"),
    (r"::error::.*", "explicit_workflow_error"),
    (r"Process completed with exit code [1-9]", "step_exit_failure"),
    (r"Unable to resolve action", "action_resolution_failure"),
    (r"cache not found|Cache not found", "cache_failure"),
]


def get_env_or_die(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: required environment variable {name} is not set", file=sys.stderr)
        sys.exit(1)
    return val


def api_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_recent_runs(owner: str, repo: str, token: str, per_page: int = 20) -> list:
    """Fetch the most recent workflow runs across the whole repo (polling mode)."""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/runs"
    resp = requests.get(url, headers=api_headers(token), params={"per_page": per_page})
    resp.raise_for_status()
    return resp.json().get("workflow_runs", [])


def get_jobs_for_run(owner: str, repo: str, run_id: int, token: str) -> list:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
    resp = requests.get(url, headers=api_headers(token))
    resp.raise_for_status()
    return resp.json().get("jobs", [])


def get_job_log(owner: str, repo: str, job_id: int, token: str) -> str:
    url = f"{GITHUB_API}/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"
    resp = requests.get(url, headers=api_headers(token))
    resp.raise_for_status()
    return resp.text


def classify_failure(log_text: str) -> list:
    matches = []
    for pattern, label in FAILURE_PATTERNS:
        found = re.findall(pattern, log_text)
        if found:
            matches.append({
                "type": label,
                "pattern": pattern,
                "occurrences": len(found),
                "sample": found[0] if isinstance(found[0], str) else str(found[0]),
            })
    return matches


def build_failure_record(owner, repo, run, job, log_text) -> dict:
    return {
        "repo": f"{owner}/{repo}",
        "workflow_name": run.get("name"),
        "run_id": run.get("id"),
        "run_number": run.get("run_number"),
        "run_url": run.get("html_url"),
        "job_id": job.get("id"),
        "job_name": job.get("name"),
        "job_conclusion": job.get("conclusion"),
        "job_started_at": job.get("started_at"),
        "job_completed_at": job.get("completed_at"),
        "runner_name": job.get("runner_name"),
        "runner_group": job.get("runner_group_name"),
        "detected_failures": classify_failure(log_text),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def push_to_elasticsearch(record: dict) -> bool:
    """Push a record into Elasticsearch using a deterministic document ID,
    so re-checking the same job never creates a duplicate — it just overwrites
    the same document. This makes both polling and event-driven triggering
    safe to run repeatedly."""
    es_endpoint = os.environ.get("ES_ENDPOINT")
    es_api_key = os.environ.get("ES_API_KEY")

    if not es_endpoint or not es_api_key:
        print("ES_ENDPOINT or ES_API_KEY not set — skipping Elasticsearch push.")
        return False

    index_name = "github-runner-failures"
    doc_id = f"{record['run_id']}-{record['job_id']}"
    url = f"{es_endpoint.rstrip('/')}/{index_name}/_doc/{doc_id}"

    headers = {
        "Authorization": f"ApiKey {es_api_key}",
        "Content-Type": "application/json",
    }

    resp = requests.put(url, headers=headers, json=record)

    if resp.status_code in (200, 201):
        print(f"Pushed to Elasticsearch as document '{doc_id}'.")
        return True
    else:
        print(f"Failed to push to Elasticsearch: {resp.status_code} {resp.text}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Fetch and classify failures from a monitored repo")
    parser.add_argument("--owner", required=True, help="Repository owner (e.g. the client's GitHub org/user)")
    parser.add_argument("--repo", required=True, help="Repository name to monitor")
    parser.add_argument("--per-page", type=int, default=20, help="How many recent runs to check each poll")
    parser.add_argument("--run-id", type=int, help="Specific run ID (for event-driven triggering)")
    parser.add_argument("--output-dir", default="data/failures", help="Where to write local JSON copies")
    args = parser.parse_args()

    token = get_env_or_die("CLIENT_REPO_TOKEN")

    if args.run_id:
        url = f"{GITHUB_API}/repos/{args.owner}/{args.repo}/actions/runs/{args.run_id}"
        resp = requests.get(url, headers=api_headers(token))
        resp.raise_for_status()
        runs = [resp.json()]
        print(f"Event-driven mode: targeting run #{args.run_id}")
    else:
        runs = get_recent_runs(args.owner, args.repo, token, per_page=args.per_page)
        print(f"Polling mode: checked {len(runs)} recent run(s) in {args.owner}/{args.repo}")

    os.makedirs(args.output_dir, exist_ok=True)
    pushed_count = 0

    for run in runs:
        if run.get("status") != "completed":
            continue
        if run.get("conclusion") not in ("failure", "cancelled", "timed_out"):
            continue

        jobs = get_jobs_for_run(args.owner, args.repo, run["id"], token)

        for job in jobs:
            if job.get("conclusion") not in ("failure", "cancelled", "timed_out"):
                continue

            log_text = get_job_log(args.owner, args.repo, job["id"], token)
            record = build_failure_record(args.owner, args.repo, run, job, log_text)

            out_path = os.path.join(args.output_dir, f"run-{run['id']}-job-{job['id']}.json")
            with open(out_path, "w") as f:
                json.dump(record, f, indent=2)
            print(f"Wrote {out_path} ({len(record['detected_failures'])} failure pattern(s) matched)")

            if push_to_elasticsearch(record):
                pushed_count += 1

    print(f"Done. {pushed_count} failure record(s) pushed to Elasticsearch.")


if __name__ == "__main__":
    main()