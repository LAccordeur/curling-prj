#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
from pathlib import Path


LATENCY_FIELDS = [
    "admission_latency_s",
    "completion_latency_s",
    "operation_latency_s",
    "upload_latency_s",
]


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * pct / 100)
    return ordered[index]


def numeric(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def metrics(values):
    values = [value for value in values if value is not None]
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "avg": statistics.mean(values),
        "median": statistics.median(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "min": min(values),
        "max": max(values),
    }


def inc(counter, key):
    counter[key] = counter.get(key, 0) + 1


def empty_group():
    return {
        "records": [],
        "by_op": {},
        "by_final_state": {},
        "by_object_size_label": {},
        "successes": 0,
        "failures": 0,
    }


def summarize_group(rows, elapsed_s=None):
    result = {
        "total": len(rows),
        "successes": sum(1 for row in rows if row.get("success") == "True"),
        "failures": sum(1 for row in rows if row.get("success") != "True"),
        "by_op": {},
        "by_final_state": {},
        "by_object_size_label": {},
        "latency_s": {},
    }
    for row in rows:
        inc(result["by_op"], row.get("op", "unknown"))
        inc(result["by_final_state"], row.get("final_state", "unknown"))
        inc(result["by_object_size_label"], row.get("object_size_label", "unknown"))

    for field in LATENCY_FIELDS:
        result["latency_s"][field] = metrics(numeric(row.get(field)) for row in rows)

    if elapsed_s and elapsed_s > 0:
        result["throughput_completed_per_s"] = result["successes"] / elapsed_s
    return result


def load_rows(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def summarize(csv_path, elapsed_s=None):
    rows = load_rows(csv_path)
    by_domain = {}
    for row in rows:
        domain_id = row.get("domain_id") or "default"
        by_domain.setdefault(domain_id, []).append(row)

    return {
        "csv": str(csv_path),
        "total": len(rows),
        "domains": {
            domain_id: summarize_group(domain_rows, elapsed_s)
            for domain_id, domain_rows in sorted(by_domain.items())
        },
        "overall": summarize_group(rows, elapsed_s),
    }


def print_table(summary):
    header = [
        "domain",
        "total",
        "success",
        "fail",
        "completion_p50",
        "completion_p95",
        "operation_p50",
        "operation_p95",
        "upload_p95",
    ]
    print("\t".join(header))
    for domain_id, data in summary["domains"].items():
        completion = data["latency_s"]["completion_latency_s"]
        operation = data["latency_s"]["operation_latency_s"]
        upload = data["latency_s"]["upload_latency_s"]
        row = [
            domain_id,
            str(data["total"]),
            str(data["successes"]),
            str(data["failures"]),
            fmt(completion.get("p50")),
            fmt(completion.get("p95")),
            fmt(operation.get("p50")),
            fmt(operation.get("p95")),
            fmt(upload.get("p95")),
        ]
        print("\t".join(row))


def fmt(value):
    if value is None:
        return ""
    return f"{value:.6f}"


def main():
    parser = argparse.ArgumentParser(description="Summarize benchmark CSV metrics by domain_id.")
    parser.add_argument("csv", help="Benchmark CSV produced by run_benchmark.py")
    parser.add_argument("--elapsed-s", type=float, default=None, help="Optional elapsed seconds for throughput calculation.")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--table", action="store_true", help="Print a compact tab-separated table.")
    args = parser.parse_args()

    summary = summarize(Path(args.csv), args.elapsed_s)
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"wrote {output_path}")

    if args.table:
        print_table(summary)
    else:
        print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
