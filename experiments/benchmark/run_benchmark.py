#!/usr/bin/env python3
import argparse
import csv
import json
import os
import random
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests


TERMINAL_STATES = {"RELEASED", "COMMITTED", "FAILED", "REJECTED"}
SUCCESS_STATES = {"read": "RELEASED", "write": "COMMITTED"}
BUILTIN_WORKLOADS = {
    "read-heavy": {"read_ratio": 0.9},
    "mixed": {"read_ratio": 0.5},
    "write-heavy": {"read_ratio": 0.1},
}
DEFAULT_SIZE_PROFILE = {
    "small": 1024,
    "medium": 1024 * 1024,
    "large": 10 * 1024 * 1024,
}
DEFAULT_SIZE_MIX = {
    "small": 0.7,
    "medium": 0.2,
    "large": 0.1,
}


def now():
    return time.perf_counter()


def wall_time():
    return time.time()


def percentile(values, pct):
    if not values:
        return None
    sorted_values = sorted(values)
    index = int((len(sorted_values) - 1) * pct / 100)
    return sorted_values[index]


def make_payload(size_bytes):
    prefix = f"benchmark-{uuid.uuid4()}-".encode("utf-8")
    if len(prefix) >= size_bytes:
        return prefix[:size_bytes]
    return prefix + (b"x" * (size_bytes - len(prefix)))


def parse_size_spec(spec, defaults):
    values = dict(defaults)
    if not spec:
        return values
    for part in spec.split(","):
        if not part.strip():
            continue
        name, value = part.split("=", 1)
        values[name.strip()] = int(value.strip())
    return values


def parse_ratio_spec(spec, defaults):
    values = dict(defaults)
    if not spec:
        return values
    values = {}
    for part in spec.split(","):
        if not part.strip():
            continue
        name, value = part.split(":", 1)
        values[name.strip()] = float(value.strip())
    total = sum(values.values())
    if total <= 0:
        raise ValueError("size mix ratios must sum to a positive value")
    return {name: ratio / total for name, ratio in values.items()}


def choose_size(size_mix, rng):
    point = rng.random()
    cumulative = 0
    last_name = None
    for name, ratio in size_mix.items():
        last_name = name
        cumulative += ratio
        if point <= cumulative:
            return name
    return last_name


def get_json(api, path, timeout):
    response = requests.get(f"{api}{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()


def post_json(api, path, payload, timeout):
    response = requests.post(f"{api}{path}", json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()

def post_empty(api, path, timeout):
    response = requests.post(f"{api}{path}", timeout=timeout)
    response.raise_for_status()
    return response.json()


def wait_for_terminal(api, request_id, timeout, interval):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = get_json(api, f"/requests/{request_id}", timeout=30)
        if last.get("state") in TERMINAL_STATES:
            return last
        time.sleep(interval)
    return last or {"request_id": request_id, "state": "TIMEOUT"}


def run_read(api, task, args):
    record = {
        "index": task["index"],
        "mode": args.mode,
        "workload": args.workload_name,
        "op": "read",
        "domain_id": task.get("domain_id", args.domain_id),
        "target": task["target"],
        "priority": task.get("priority", 0),
        "object_size_label": task["object_size_label"],
        "object_size_bytes": task["object_size_bytes"],
        "upload_latency_s": "",
        "error": "",
    }
    operation_start = now()
    submit_start_wall = wall_time()
    submit_start = now()
    try:
        response = post_json(
            api,
            "/requests/read",
            {
                "requester_id": args.requester_id,
                "domain_id": task.get("domain_id", args.domain_id),
                "target_spec": task["target"],
                "priority": task.get("priority", 0),
                "reuse_enabled": True,
            },
            timeout=args.http_timeout,
        )
        submit_end = now()
        record["request_id"] = response.get("request_id", "")
        record["submit_state"] = response.get("state", "")
        record["admission_latency_s"] = submit_end - submit_start

        final = response
        if response.get("state") not in TERMINAL_STATES:
            final = wait_for_terminal(api, response["request_id"], args.timeout, args.poll_interval)
        operation_end = now()
        record["final_state"] = final.get("state", "UNKNOWN")
        record["success"] = record["final_state"] == SUCCESS_STATES["read"]
        record["completion_latency_s"] = operation_end - submit_start
        record["operation_latency_s"] = operation_end - operation_start
        record["submit_start_epoch"] = submit_start_wall
        return record
    except Exception as exc:
        operation_end = now()
        record.update({
            "request_id": record.get("request_id", ""),
            "submit_state": record.get("submit_state", ""),
            "final_state": "ERROR",
            "success": False,
            "admission_latency_s": record.get("admission_latency_s", ""),
            "completion_latency_s": operation_end - submit_start,
            "operation_latency_s": operation_end - operation_start,
            "submit_start_epoch": submit_start_wall,
            "error": str(exc),
        })
        return record


def run_write(api, task, args):
    record = {
        "index": task["index"],
        "mode": args.mode,
        "workload": args.workload_name,
        "op": "write",
        "domain_id": task.get("domain_id", args.domain_id),
        "target": task["target"],
        "input_path": task.get("input_path", ""),
        "priority": task.get("priority", 0),
        "object_size_label": task["object_size_label"],
        "object_size_bytes": task["object_size_bytes"],
        "error": "",
    }
    operation_start = now()
    submit_start_wall = wall_time()
    upload_id = ""
    try:
        if task.get("input_path"):
            payload = Path(task["input_path"]).read_bytes()
        else:
            payload = make_payload(task["object_size_bytes"])
        upload_start = now()
        upload = post_json(
            api,
            "/uploads/init",
            {
                "requester_id": args.requester_id,
                "domain_id": task.get("domain_id", args.domain_id),
                "content_type": "application/octet-stream",
                "size_bytes": len(payload),
                "filename": f"benchmark-{task['index']}.bin",
            },
            timeout=args.http_timeout,
        )
        upload_id = upload["upload_id"]
        record["upload_id"] = upload_id
        put_response = requests.put(
            upload["upload_url"],
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            timeout=args.http_timeout,
        )
        put_response.raise_for_status()
        upload_end = now()

        submit_start = now()
        response = post_json(
            api,
            "/requests/write",
            {
                "requester_id": args.requester_id,
                "domain_id": task.get("domain_id", args.domain_id),
                "upload_id": upload_id,
                "target_spec": task["target"],
                "priority": task.get("priority", 0),
                "reuse_enabled": True,
            },
            timeout=args.http_timeout,
        )
        submit_end = now()
        record["request_id"] = response.get("request_id", "")
        record["submit_state"] = response.get("state", "")
        record["upload_latency_s"] = upload_end - upload_start
        record["admission_latency_s"] = submit_end - submit_start

        final = response
        if response.get("state") not in TERMINAL_STATES:
            final = wait_for_terminal(api, response["request_id"], args.timeout, args.poll_interval)
        operation_end = now()
        record["final_state"] = final.get("state", "UNKNOWN")
        record["success"] = record["final_state"] == SUCCESS_STATES["write"]
        record["completion_latency_s"] = operation_end - submit_start
        record["operation_latency_s"] = operation_end - operation_start
        record["submit_start_epoch"] = submit_start_wall
        return record
    except Exception as exc:
        operation_end = now()
        abort_error = ""
        if upload_id and not record.get("request_id"):
            try:
                post_empty(api, f"/uploads/{upload_id}/abort", timeout=args.http_timeout)
            except Exception as abort_exc:
                abort_error = f"; abort_upload_error={abort_exc}"
        record.update({
            "request_id": record.get("request_id", ""),
            "upload_id": upload_id,
            "submit_state": record.get("submit_state", ""),
            "final_state": "ERROR",
            "success": False,
            "upload_latency_s": record.get("upload_latency_s", ""),
            "admission_latency_s": record.get("admission_latency_s", ""),
            "completion_latency_s": operation_end - operation_start,
            "operation_latency_s": operation_end - operation_start,
            "submit_start_epoch": submit_start_wall,
            "error": f"{exc}{abort_error}",
        })
        return record


def load_workload(args):
    data = BUILTIN_WORKLOADS[args.workload]
    read_ratio = data["read_ratio"]
    size_profile = parse_size_spec(args.size_profile, DEFAULT_SIZE_PROFILE)
    size_mix = parse_ratio_spec(args.size_mix, DEFAULT_SIZE_MIX)
    name = args.workload
    args.workload_name = name
    missing_sizes = set(size_mix) - set(size_profile)
    if missing_sizes:
        raise ValueError(f"size mix references undefined size labels: {sorted(missing_sizes)}")
    return read_ratio, size_profile, size_mix


def make_tasks(args):
    if args.trace_json:
        return load_trace_tasks(args)

    read_ratio, size_profile, size_mix = load_workload(args)
    args.effective_size_profile = size_profile
    args.effective_size_mix = size_mix
    read_targets = [target.strip() for target in args.read_targets.split(",") if target.strip()]
    if not read_targets:
        raise ValueError("at least one read target is required")

    tasks = []
    rng = random.Random(args.seed)
    read_count = round(args.total_requests * read_ratio)
    write_count = args.total_requests - read_count
    for index in range(read_count):
        size_label = choose_size(size_mix, rng)
        tasks.append({
            "index": index,
            "op": "read",
            "domain_id": args.domain_id,
            "target": read_targets[index % len(read_targets)],
            "priority": 0,
            "object_size_label": size_label,
            "object_size_bytes": size_profile[size_label],
        })
    for offset in range(write_count):
        index = read_count + offset
        size_label = choose_size(size_mix, rng)
        tasks.append({
            "index": index,
            "op": "write",
            "domain_id": args.domain_id,
            "target": f"{args.write_target_prefix}_{index}_{uuid.uuid4().hex[:8]}",
            "priority": 0,
            "object_size_label": size_label,
            "object_size_bytes": size_profile[size_label],
        })
    rng.shuffle(tasks)
    for index, task in enumerate(tasks):
        task["index"] = index
    return tasks


def load_trace_tasks(args):
    trace_path = Path(args.trace_json)
    with open(trace_path, "r", encoding="utf-8") as fh:
        trace = json.load(fh)

    tasks = []
    base_dir = Path(args.trace_base_dir) if args.trace_base_dir else trace_path.parent
    for index, request in enumerate(trace["requests"]):
        op = request["type"]
        target = request.get("target_spec") or request.get("target")
        if not target:
            raise ValueError(f"trace request {index} is missing target_spec")

        task = {
            "index": index,
            "op": op,
            "domain_id": request.get("domain_id") or trace.get("domain_id", args.domain_id),
            "target": target,
            "priority": int(request.get("priority", 0)),
            "object_size_label": request.get("size_label", "unknown"),
            "object_size_bytes": int(request.get("size_bytes", 0)),
        }
        if op == "write":
            input_path = Path(request["input_path"])
            if not input_path.is_absolute():
                input_path = base_dir / input_path
            task["input_path"] = str(input_path)
            if task["object_size_bytes"] == 0:
                task["object_size_bytes"] = input_path.stat().st_size
        elif op != "read":
            raise ValueError(f"unsupported trace request type at index {index}: {op}")
        tasks.append(task)

    args.workload_name = trace.get("name", trace_path.stem)
    args.total_requests = len(tasks)
    args.effective_size_profile = trace.get("size_profile", {})
    args.effective_size_mix = trace.get("size_mix", {})
    return tasks


def summarize(records, elapsed_s):
    successes = [r for r in records if r.get("success")]
    failures = [r for r in records if not r.get("success")]

    def metric(name):
        values = [r[name] for r in records if isinstance(r.get(name), (int, float))]
        if not values:
            return {"count": 0}
        return {
            "count": len(values),
            "avg": statistics.mean(values),
            "median": statistics.median(values),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
            "min": min(values),
            "max": max(values),
        }

    return {
        "total": len(records),
        "successes": len(successes),
        "failures": len(failures),
        "elapsed_s": elapsed_s,
        "throughput_completed_per_s": len(successes) / elapsed_s if elapsed_s > 0 else 0,
        "by_final_state": {state: sum(1 for r in records if r.get("final_state") == state) for state in sorted({r.get("final_state") for r in records})},
        "by_domain_id": {domain_id: sum(1 for r in records if r.get("domain_id") == domain_id) for domain_id in sorted({r.get("domain_id") for r in records})},
        "by_object_size_label": {label: sum(1 for r in records if r.get("object_size_label") == label) for label in sorted({r.get("object_size_label") for r in records})},
        "admission_latency_s": metric("admission_latency_s"),
        "completion_latency_s": metric("completion_latency_s"),
        "operation_latency_s": metric("operation_latency_s"),
        "upload_latency_s": metric("upload_latency_s"),
    }


def write_outputs(args, records, summary):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    base = f"{stamp}-{args.mode}-{args.workload_name}-c{args.concurrency}-n{args.total_requests}"
    csv_path = output_dir / f"{base}.csv"
    json_path = output_dir / f"{base}.summary.json"

    fieldnames = [
        "index", "mode", "workload", "op", "domain_id", "target", "request_id", "upload_id", "submit_state", "final_state",
        "success", "input_path", "priority", "object_size_label", "object_size_bytes", "admission_latency_s", "completion_latency_s", "operation_latency_s",
        "upload_latency_s", "submit_start_epoch", "error",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for record in sorted(records, key=lambda item: item["index"]):
            writer.writerow({name: record.get(name, "") for name in fieldnames})

    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)

    return csv_path, json_path


def main():
    parser = argparse.ArgumentParser(description="Run an end-to-end Curling benchmark against the currently configured execution mode.")
    parser.add_argument("--mode", required=True, help="Label for the running mode, e.g. coupled-rpc or decoupled-reuse.")
    parser.add_argument("--api", default=os.getenv("CONTROL_API", "http://127.0.0.1:8000"))
    parser.add_argument("--workload", choices=sorted(BUILTIN_WORKLOADS), default="mixed")
    parser.add_argument("--trace-json", help="Manifest generated by generate_dataset.py. Overrides built-in workload generation.")
    parser.add_argument("--trace-base-dir", help="Base directory for relative write input paths in --trace-json.")
    parser.add_argument("--total-requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--read-targets", default=os.getenv("BENCH_READ_TARGETS", "studyA"))
    parser.add_argument("--write-target-prefix", default=os.getenv("BENCH_WRITE_TARGET_PREFIX", "bench_write"))
    parser.add_argument("--domain-id", default=os.getenv("BENCH_DOMAIN_ID", "default"))
    parser.add_argument("--size-profile", default=os.getenv("BENCH_SIZE_PROFILE"))
    parser.add_argument("--size-mix", default=os.getenv("BENCH_SIZE_MIX"))
    parser.set_defaults(requester_id="benchmark-user")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--http-timeout", type=int, default=300)
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--output-dir", default="experiments/benchmark/results")
    args = parser.parse_args()

    health = get_json(args.api, "/health", timeout=args.http_timeout)
    if not health.get("ok"):
        raise RuntimeError(f"API health check failed: {health}")

    tasks = make_tasks(args)
    start = now()
    records = []
    print(args)
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(run_read if task["op"] == "read" else run_write, args.api, task, args)
            for task in tasks
        ]
        for future in as_completed(futures):
            record = future.result()
            records.append(record)
            message = (
                f"{record['op']} index={record['index']} state={record.get('final_state')} "
                f"success={record.get('success')} request_id={record.get('request_id')}"
            )
            if record.get("final_state") == "ERROR" or record.get("error"):
                message = f"{message} error={record.get('error')}"
            print(
                message,
                flush=True,
            )
    elapsed = now() - start
    summary = summarize(records, elapsed)
    summary.update({
        "mode": args.mode,
        "workload": args.workload_name,
        "concurrency": args.concurrency,
        "total_requests": len(tasks),
        "api": args.api,
        "trace_json": args.trace_json or "",
        "size_mix": args.effective_size_mix,
        "size_profile": args.effective_size_profile,
    })
    csv_path, json_path = write_outputs(args, records, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"wrote {csv_path}")
    print(f"wrote {json_path}")

    #if summary["failures"]:
    #    sys.exit(1)


if __name__ == "__main__":
    main()
