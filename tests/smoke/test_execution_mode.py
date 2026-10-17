#!/usr/bin/env python3
import argparse
import os
import sys
import time
import uuid

import requests


TERMINAL_STATES = {"RELEASED", "COMMITTED", "FAILED", "REJECTED"}
MODE_SUBMIT_STATES = {
    "coupled-rpc": {"read": {"RELEASED", "FAILED"}, "write": {"COMMITTED", "FAILED", "REJECTED"}},
    "coupled-storage-api": {"read": {"RELEASED", "FAILED"}, "write": {"COMMITTED", "FAILED", "REJECTED"}},
    "decoupled-baseline": {"read": {"ENQUEUED"}, "write": {"ENQUEUED"}},
    "decoupled-reuse": {"read": {"ENQUEUED"}, "write": {"ENQUEUED"}},
}


def post_json(api, path, payload, timeout):
    response = requests.post(f"{api}{path}", json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_json(api, path, timeout):
    response = requests.get(f"{api}{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()


def wait_for_state(api, request_id, expected_states, timeout, interval):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = get_json(api, f"/requests/{request_id}", timeout=30)
        state = last.get("state")
        if state in expected_states:
            return last
        if state in TERMINAL_STATES:
            raise AssertionError(f"request {request_id} reached unexpected terminal state {state}: {last}")
        time.sleep(interval)
    raise TimeoutError(f"request {request_id} did not reach {expected_states}; last={last}")


def assert_submit_state(mode, request_type, response):
    expected = MODE_SUBMIT_STATES[mode][request_type]
    state = response.get("state")
    if state not in expected:
        raise AssertionError(f"{mode} {request_type} submit returned state={state}, expected one of {sorted(expected)}")


def test_read(api, mode, target, timeout, interval):
    response = post_json(
        api,
        "/requests/read",
        {
            "requester_id": "smoke-user",
            "target_spec": target,
            "priority": 1,
            "reuse_enabled": True,
        },
        timeout=30,
    )
    assert_submit_state(mode, "read", response)
    request_id = response["request_id"]
    final = response
    if response.get("state") != "RELEASED":
        final = wait_for_state(api, request_id, {"RELEASED"}, timeout, interval)
    print(f"read ok request_id={request_id} state={final.get('state')}")


def test_write(api, mode, target, timeout, interval):
    upload_payload = f"smoke-write-{uuid.uuid4()}".encode("utf-8")
    upload = post_json(
        api,
        "/uploads/init",
        {
            "requester_id": "smoke-user",
            "content_type": "application/octet-stream",
            "size_bytes": len(upload_payload),
            "filename": "smoke.bin",
        },
        timeout=30,
    )
    put_response = requests.put(
        upload["upload_url"],
        data=upload_payload,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    put_response.raise_for_status()

    response = post_json(
        api,
        "/requests/write",
        {
            "requester_id": "smoke-user",
            "upload_id": upload["upload_id"],
            "target_spec": target,
            "priority": 1,
            "reuse_enabled": True,
        },
        timeout=30,
    )
    assert_submit_state(mode, "write", response)
    request_id = response["request_id"]
    final = response
    if response.get("state") != "COMMITTED":
        final = wait_for_state(api, request_id, {"COMMITTED"}, timeout, interval)
    print(f"write ok request_id={request_id} state={final.get('state')}")


def main():
    parser = argparse.ArgumentParser(description="Smoke test one configured execution mode end to end.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=sorted(MODE_SUBMIT_STATES),
        help="Expected running mode. Start services in this mode before running the test.",
    )
    parser.add_argument("--api", default=os.getenv("CONTROL_API", "http://127.0.0.1:8000"))
    parser.add_argument("--read-target", default=os.getenv("SMOKE_READ_TARGET", "studyA"))
    parser.add_argument("--write-target", default=os.getenv("SMOKE_WRITE_TARGET", f"smoke_{uuid.uuid4().hex}"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("SMOKE_TIMEOUT_SECONDS", "120")))
    parser.add_argument("--interval", type=float, default=float(os.getenv("SMOKE_POLL_INTERVAL_SECONDS", "2")))
    args = parser.parse_args()

    health = get_json(args.api, "/health", timeout=30)
    if not health.get("ok"):
        raise AssertionError(f"API health check failed: {health}")

    print(f"testing mode={args.mode} api={args.api}")
    test_read(args.api, args.mode, args.read_target, args.timeout, args.interval)
    test_write(args.api, args.mode, args.write_target, args.timeout, args.interval)
    print("execution mode smoke test passed")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"execution mode smoke test failed: {exc}", file=sys.stderr)
        sys.exit(1)
