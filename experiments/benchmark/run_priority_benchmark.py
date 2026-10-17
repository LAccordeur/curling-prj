#!/usr/bin/env python3
import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
BENCH_DIR = ROOT_DIR / "experiments" / "benchmark"
GENERATE_DATASET = BENCH_DIR / "generate_dataset.py"
RUN_BENCHMARK = BENCH_DIR / "run_benchmark.py"


def run_cmd(cmd, cwd=ROOT_DIR):
    print("+ " + " ".join(str(part) for part in cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def run_shell(cmd, cwd=ROOT_DIR):
    print("+ " + cmd, flush=True)
    subprocess.run(cmd, cwd=cwd, shell=True, check=True)


def latest_file(directory, pattern, since):
    matches = [
        path for path in Path(directory).glob(pattern)
        if path.stat().st_mtime >= since
    ]
    if not matches:
        raise RuntimeError(f"no output file matching {pattern} in {directory}")
    return max(matches, key=lambda path: path.stat().st_mtime)


def generate_dataset(args, name, priority_policy):
    output_dir = Path(args.generated_dir) / name
    write_input_dir = output_dir / "writes"
    trace_json = output_dir / f"{name}.trace.json"

    common = [
        sys.executable,
        str(GENERATE_DATASET),
        "--name", name,
        "--total-requests", str(args.total_requests),
        "--read-ratio", str(args.read_ratio),
        "--read-target-count", str(args.read_target_count),
        "--read-repeat-mode", args.read_repeat_mode,
        "--zipf-alpha", str(args.zipf_alpha),
        "--size-profile", args.size_profile,
        "--size-mix", args.size_mix,
        "--domain-id", args.domain_id,
        "--domain-mix", args.domain_mix,
        "--seed", str(args.seed),
    ]
    if priority_policy == "uniform":
        priority_args = [
            "--priority-policy", "uniform",
            "--uniform-priority", str(args.uniform_priority),
        ]
    else:
        priority_args = [
            "--priority-policy", "small-first",
            "--priority-by-size", args.priority_by_size,
        ]

    run_cmd(common + priority_args + [
        "--role", "admin",
        "--output-dir", str(output_dir),
    ])
    run_cmd([
        sys.executable,
        str(GENERATE_DATASET),
        "--role", "private",
        "--trace-json", str(trace_json),
        "--private-export-dir", args.private_export_dir,
    ])
    if args.sync_command:
        run_shell(args.sync_command)
    run_cmd([
        sys.executable,
        str(GENERATE_DATASET),
        "--role", "client",
        "--trace-json", str(trace_json),
        "--write-input-dir", str(write_input_dir),
    ])
    return trace_json, output_dir


def run_benchmark(args, label, trace_json, trace_base_dir):
    if args.reset_command:
        run_shell(args.reset_command)
        if args.reset_sleep > 0:
            time.sleep(args.reset_sleep)

    started_at = time.time()
    run_cmd([
        sys.executable,
        str(RUN_BENCHMARK),
        "--mode", label,
        "--api", args.api,
        "--trace-json", str(trace_json),
        "--trace-base-dir", str(trace_base_dir),
        "--concurrency", str(args.concurrency),
        "--timeout", str(args.timeout),
        "--http-timeout", str(args.http_timeout),
        "--poll-interval", str(args.poll_interval),
        "--output-dir", args.output_dir,
    ])
    csv_path = latest_file(args.output_dir, f"*-{label}-*.csv", started_at)
    json_path = latest_file(args.output_dir, f"*-{label}-*.summary.json", started_at)
    return csv_path, json_path


def percentile(values, pct):
    if not values:
        return None
    values = sorted(values)
    index = int((len(values) - 1) * pct / 100)
    return values[index]


def summarize_csv(csv_path):
    groups = {}
    all_success = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("success") != "True":
                continue
            completion = float(row["completion_latency_s"])
            operation = float(row["operation_latency_s"])
            size = row.get("object_size_label", "unknown")
            priority = row.get("priority", "unknown")
            keys = [
                ("all", "all"),
                ("size", size),
                ("priority", priority),
                ("size_priority", f"{size}/p{priority}"),
            ]
            all_success.append(row)
            for kind, name in keys:
                groups.setdefault(kind, {}).setdefault(name, {
                    "completion_latency_s": [],
                    "operation_latency_s": [],
                })
                groups[kind][name]["completion_latency_s"].append(completion)
                groups[kind][name]["operation_latency_s"].append(operation)

    def metrics(values):
        if not values:
            return {"count": 0}
        return {
            "count": len(values),
            "avg": sum(values) / len(values),
            "p50": percentile(values, 50),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
            "min": min(values),
            "max": max(values),
        }

    result = {}
    for kind, entries in groups.items():
        result[kind] = {}
        for name, values in entries.items():
            result[kind][name] = {
                "completion_latency_s": metrics(values["completion_latency_s"]),
                "operation_latency_s": metrics(values["operation_latency_s"]),
            }
    result["success_count"] = len(all_success)
    return result


def compare(priority_summary, uniform_summary):
    def add_metric_pair(entry, metric_name, uniform_value, priority_value):
        if priority_value is None or uniform_value is None:
            return
        entry[f"uniform_completion_{metric_name}_s"] = uniform_value
        entry[f"priority_completion_{metric_name}_s"] = priority_value
        if uniform_value != 0:
            entry[f"{metric_name}_improvement_ratio"] = (uniform_value - priority_value) / uniform_value

    comparison = {}
    for group in ("size", "all"):
        comparison[group] = {}
        priority_groups = priority_summary.get(group, {})
        uniform_groups = uniform_summary.get(group, {})
        for name, priority_metrics in priority_groups.items():
            if name not in uniform_groups:
                continue
            priority_completion = priority_metrics["completion_latency_s"]
            uniform_completion = uniform_groups[name]["completion_latency_s"]
            priority_p95 = priority_completion.get("p95")
            uniform_p95 = uniform_completion.get("p95")
            if priority_p95 is None or uniform_p95 is None:
                continue
            entry = {
                "count": priority_completion.get("count", 0),
                "uniform_count": uniform_completion.get("count", 0),
                "priority_count": priority_completion.get("count", 0),
            }
            for metric_name in ("avg", "p50", "p95"):
                add_metric_pair(
                    entry,
                    metric_name,
                    uniform_completion.get(metric_name),
                    priority_completion.get(metric_name),
                )
            comparison[group][name] = entry
    return comparison


def main():
    parser = argparse.ArgumentParser(
        description="Generate and run a paired priority scheduling benchmark."
    )
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    parser.add_argument("--mode", default="priority-scheduling")
    parser.add_argument("--generated-dir", default="experiments/benchmark/generated/priority")
    parser.add_argument("--output-dir", default="experiments/benchmark/results_priority")
    parser.add_argument("--private-export-dir", default="private-data/exports")
    parser.add_argument("--sync-command", default="", help="Optional shell command after private read files are generated, e.g. bash sync_to_nodes.sh")
    parser.add_argument("--reset-command", default="", help="Optional shell command before each benchmark run.")
    parser.add_argument("--reset-sleep", type=float, default=3.0)
    parser.add_argument("--total-requests", type=int, default=120)
    parser.add_argument("--read-ratio", type=float, default=1.0)
    parser.add_argument("--read-target-count", type=int, default=120)
    parser.add_argument("--read-repeat-mode", choices=["uniform", "zipf", "unique"], default="unique")
    parser.add_argument("--zipf-alpha", type=float, default=0.0)
    parser.add_argument("--size-profile", default="small=4096,medium=8388608,large=67108864")
    parser.add_argument("--size-mix", default="small:0.5,medium:0.3,large:0.2")
    parser.add_argument("--priority-by-size", default="small:2,medium:1,large:0")
    parser.add_argument("--uniform-priority", type=int, default=0)
    parser.add_argument("--domain-id", default="default")
    parser.add_argument("--domain-mix", default="")
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--http-timeout", type=int, default=300)
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    uniform_trace, uniform_base = generate_dataset(args, "priority-uniform", "uniform")
    priority_trace, priority_base = generate_dataset(args, "priority-small-first", "small-first")

    uniform_csv, uniform_json = run_benchmark(args, f"{args.mode}-uniform", uniform_trace, uniform_base)
    priority_csv, priority_json = run_benchmark(args, f"{args.mode}-small-first", priority_trace, priority_base)

    uniform_summary = summarize_csv(uniform_csv)
    priority_summary = summarize_csv(priority_csv)
    report = {
        "uniform": {
            "csv": str(uniform_csv),
            "summary_json": str(uniform_json),
            "metrics": uniform_summary,
        },
        "small_first_priority": {
            "csv": str(priority_csv),
            "summary_json": str(priority_json),
            "metrics": priority_summary,
        },
        "comparison": compare(priority_summary, uniform_summary),
        "notes": [
            "Positive p95_improvement_ratio means the size-aware priority run reduced completion p95.",
            "Priority scheduling is non-preemptive and applies within scheduler batch windows.",
            "Use EXECUTOR_SCHEDULER_BATCH_SIZE and EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS to control the scheduler window.",
        ],
    }

    report_path = Path(args.output_dir) / f"{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{args.mode}-priority-report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"wrote {report_path}")


if __name__ == "__main__":
    main()
