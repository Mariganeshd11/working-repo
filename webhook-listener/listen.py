#!/usr/bin/env python3
"""
Connects to a smee.io channel, listens for GitHub webhook events forwarded
from client-repo, and triggers working-repo's workflow instantly when a
workflow run fails.

NOTE ON SIGNATURE VERIFICATION:
smee.io parses the incoming webhook body as JSON and re-serializes it before
forwarding it over SSE. This means the exact original bytes GitHub used to
compute its HMAC signature are not preserved, so byte-exact signature
verification against a smee.io-relayed payload is NOT reliable in general.
This is a known limitation of smee.io itself (documented in smee.io/probot
issue trackers), not a bug in this script.

For this reason, signature verification here is best-effort only: it is
attempted and logged, but a mismatch does NOT block processing, since a
mismatch is expected/normal when relayed through smee.io. This is acceptable
for local development and demonstration purposes. It is NOT a substitute for
proper signature verification in a real production webhook receiver, which
must receive the truly raw, unmodified request body directly from GitHub.
"""

import os
import sys
import json
import hmac
import hashlib
import requests

SMEE_URL = os.environ["SMEE_URL"]                # e.g. https://smee.io/AbCdEf123456
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]     # shared secret, also set on the GitHub webhook
WORKING_REPO_TOKEN = os.environ["WORKING_REPO_TOKEN"]
WORKING_REPO_OWNER = os.environ["WORKING_REPO_OWNER"]
WORKING_REPO_NAME = os.environ["WORKING_REPO_NAME"]


def best_effort_signature_check(raw_body: bytes, signature_header: str) -> bool:
    """Attempts HMAC verification and logs the result, but never blocks
    processing on its own — see module docstring for why."""
    if not signature_header:
        print("No signature header present on this event.", file=sys.stderr)
        return False
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256
    ).hexdigest()
    matched = hmac.compare_digest(expected, signature_header)
    if matched:
        print("Signature matched.")
    else:
        print(
            "Signature did not match (expected with smee.io relaying — "
            "see module docstring). Proceeding anyway for local testing.",
            file=sys.stderr,
        )
    return matched


def trigger_working_repo(run_id: int, client_repo_full_name: str):
    url = f"https://api.github.com/repos/{WORKING_REPO_OWNER}/{WORKING_REPO_NAME}/dispatches"
    headers = {
        "Authorization": f"Bearer {WORKING_REPO_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    body = {
        "event_type": "client_repo_failure",
        "client_payload": {
            "run_id": run_id,
            "repo": client_repo_full_name,
        },
    }
    resp = requests.post(url, headers=headers, json=body, timeout=30)
    if resp.status_code == 204:
        print(f"Triggered working-repo for run {run_id}")
    else:
        print(f"Failed to trigger working-repo: {resp.status_code} {resp.text}", file=sys.stderr)


def iter_sse_events(response):
    """Yield Server-Sent Events data without requiring sseclient."""
    data_lines = []
    for line in response.iter_lines(decode_unicode=True):
        if not line:
            if data_lines:
                yield "\n".join(data_lines)
                data_lines = []
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())

    if data_lines:
        yield "\n".join(data_lines)


def main():
    print(f"Connecting to {SMEE_URL} ...")
    resp = requests.get(
        SMEE_URL,
        stream=True,
        headers={"Accept": "text/event-stream"},
        timeout=(10, None),
    )
    resp.raise_for_status()
    print("Connected. Waiting for events...")

    for event_data in iter_sse_events(resp):
        if not event_data:
            continue
        try:
            payload = json.loads(event_data)
        except json.JSONDecodeError:
            continue

        # smee.io wraps the original GitHub headers/body inside this payload
        headers = {
            str(key).lower(): value
            for key, value in payload.get("headers", {}).items()
        }
        body = payload.get("body", "")
        if isinstance(body, str):
            raw_body = body.encode("utf-8")
            try:
                body_json = json.loads(body)
            except json.JSONDecodeError:
                continue
        else:
            body_json = body
            raw_body = json.dumps(body, separators=(",", ":")).encode("utf-8")

        signature = headers.get("x-hub-signature-256", "")
        best_effort_signature_check(raw_body, signature)  # logged, not blocking

        github_event = headers.get("x-github-event", "")
        if github_event != "workflow_run":
            continue

        run = body_json.get("workflow_run", {})
        conclusion = run.get("conclusion")
        run_id = run.get("id")
        repo_full_name = body_json.get("repository", {}).get("full_name")

        if conclusion in ("failure", "cancelled", "timed_out"):
            print(f"Failure detected: {repo_full_name} run {run_id} ({conclusion})")
            trigger_working_repo(run_id, repo_full_name)
        else:
            print(f"Run {run_id} completed with conclusion={conclusion}, ignoring.")


if __name__ == "__main__":
    main()