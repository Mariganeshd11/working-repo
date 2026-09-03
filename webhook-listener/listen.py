#!/usr/bin/env python3
"""
Connects to a smee.io channel, listens for GitHub webhook events forwarded
from client-repo, and triggers working-repo's workflow instantly when a
workflow run fails.
"""

import os
import sys
import json
import hmac
import hashlib
import requests
import sseclient  # pip install sseclient-py

SMEE_URL = os.environ["SMEE_URL"]                # e.g. https://smee.io/AbCdEf123456
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]     # shared secret, also set on the GitHub webhook
WORKING_REPO_TOKEN = os.environ["WORKING_REPO_TOKEN"]
WORKING_REPO_OWNER = os.environ["WORKING_REPO_OWNER"]
WORKING_REPO_NAME = os.environ["WORKING_REPO_NAME"]


def verify_signature(payload_body: bytes, signature_header: str) -> bool:
    if not signature_header:
        return False
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode(), payload_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


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
    resp = requests.post(url, headers=headers, json=body)
    if resp.status_code == 204:
        print(f"Triggered working-repo for run {run_id}")
    else:
        print(f"Failed to trigger working-repo: {resp.status_code} {resp.text}", file=sys.stderr)


def main():
    print(f"Connecting to {SMEE_URL} ...")
    resp = requests.get(SMEE_URL, stream=True, headers={"Accept": "text/event-stream"})
    client = sseclient.SSEClient(resp)

    for event in client.events():
        if not event.data:
            continue
        try:
            payload = json.loads(event.data)
        except json.JSONDecodeError:
            continue

        # smee.io wraps the original GitHub headers/body inside this payload
        headers = payload.get("headers", {})
        body_json = payload.get("body", {})
        raw_body = json.dumps(body_json).encode()

        signature = headers.get("x-hub-signature-256", "")
        if not verify_signature(raw_body, signature):
            print("Signature verification failed — ignoring event.", file=sys.stderr)
            continue

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