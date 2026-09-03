#!/usr/bin/env python3

import os
import sys
import json
import hmac
import hashlib
import requests

SMEE_URL = os.environ["SMEE_URL"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
WORKING_REPO_TOKEN = os.environ["WORKING_REPO_TOKEN"]
WORKING_REPO_OWNER = os.environ["WORKING_REPO_OWNER"]
WORKING_REPO_NAME = os.environ["WORKING_REPO_NAME"]


def best_effort_signature_check(raw_body: bytes, signature_header: str) -> bool:
    if not signature_header:
        print("No signature header present on this event.", file=sys.stderr)
        return False

    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()

    matched = hmac.compare_digest(expected, signature_header)

    if matched:
        print("Signature matched.")
    else:
        print(
            "Signature did not match after smee relay. "
            "Proceeding for local demo.",
            file=sys.stderr,
        )

    return matched


def trigger_working_repo(run_id: int, client_repo_full_name: str):
    url = (
        f"https://api.github.com/repos/"
        f"{WORKING_REPO_OWNER}/{WORKING_REPO_NAME}/dispatches"
    )

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

    resp = requests.post(
        url,
        headers=headers,
        json=body,
        timeout=30,
    )

    if resp.status_code == 204:
        print(f"Triggered working-repo for run {run_id}")
    else:
        print(
            f"Failed to trigger working-repo: "
            f"{resp.status_code} {resp.text}",
            file=sys.stderr,
        )


def iter_sse_events(response):
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

        # IMPORTANT:
        # Smee puts GitHub headers at the TOP LEVEL of the SSE payload.
        signature = payload.get("x-hub-signature-256", "")
        github_event = payload.get("x-github-event", "")

        body = payload.get("body", {})

        if isinstance(body, str):
            try:
                body_json = json.loads(body)
            except json.JSONDecodeError:
                continue

            raw_body = body.encode("utf-8")
        else:
            body_json = body

            raw_body = json.dumps(
                body,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")

        best_effort_signature_check(
            raw_body,
            signature,
        )

        print(f"GitHub event: {github_event}")

        if github_event != "workflow_run":
            continue

        action = body_json.get("action")
        run = body_json.get("workflow_run", {})

        conclusion = run.get("conclusion")
        run_id = run.get("id")
        repo_full_name = body_json.get(
            "repository", {}
        ).get("full_name")

        print(
            f"workflow_run action={action}, "
            f"conclusion={conclusion}, "
            f"run_id={run_id}"
        )

        if action != "completed":
            continue

        if conclusion in ("failure", "cancelled", "timed_out"):
            print(
                f"FAILURE DETECTED: "
                f"{repo_full_name} run {run_id} ({conclusion})"
            )

            trigger_working_repo(
                run_id,
                repo_full_name,
            )

        else:
            print(
                f"Run {run_id} completed with "
                f"conclusion={conclusion}, ignoring."
            )


if __name__ == "__main__":
    main()