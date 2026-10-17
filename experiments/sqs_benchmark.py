#!/usr/bin/env python3
import argparse
import csv
import json
import os
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config


def now():
    return time.perf_counter()


def wall_time():
    return time.time()


def load_env_file(path):
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def first_csv_value(value):
    if not value:
        return ""
    return next((item.strip() for item in value.split(",") if item.strip()), "")


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = int((len(ordered) - 1) * pct / 100)
    return ordered[index]


def metrics(values):
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


def is_aws_queue_backend(backend):
    return str(backend or "").lower() in {"aws_sqs", "sqs", "aws"}


def default_queue_url():
    return first_csv_value(os.getenv("PRIVATE_DOMAIN_QUEUE_URLS")) or os.getenv("SQS_QUEUE_URL")


def default_queue_name():
    return first_csv_value(os.getenv("PRIVATE_DOMAIN_QUEUES")) or os.getenv("SQS_QUEUE_NAME")


def build_aws_queue_url(region, account_id, queue_name):
    if not account_id or not queue_name:
        return ""
    return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"


def make_sqs_client(args):
    cfg = Config(
        region_name=args.region,
        max_pool_connections=args.max_pool_connections,
        retries={"mode": "standard", "total_max_attempts": args.max_attempts},
    )
    kwargs = {"endpoint_url": args.endpoint_url or None, "config": cfg}
    if args.access_key and args.secret_key:
        kwargs["aws_access_key_id"] = args.access_key
        kwargs["aws_secret_access_key"] = args.secret_key
        if args.session_token:
            kwargs["aws_session_token"] = args.session_token
    return boto3.client("sqs", **kwargs)


def resolve_queue_url(sqs, args):
    if args.queue_url:
        return args.queue_url
    if is_aws_queue_backend(args.queue_backend):
        built = build_aws_queue_url(args.region, args.aws_account_id, args.queue_name)
        if built:
            return built
    if args.queue_name:
        return sqs.get_queue_url(QueueName=args.queue_name)["QueueUrl"]
    raise ValueError("queue URL is required; pass --queue-url or set SQS_QUEUE_URL/queue name")


def message_body(run_id, index, payload_bytes):
    header = {
        "run_id": run_id,
        "index": index,
        "sent_at_epoch_s": wall_time(),
    }
    text = json.dumps(header, separators=(",", ":"))
    if len(text) >= payload_bytes:
        return text
    return text + ("x" * (payload_bytes - len(text)))


def parse_sent_at(body):
    try:
        decoder = json.JSONDecoder()
        payload, _ = decoder.raw_decode(body)
        return float(payload.get("sent_at_epoch_s"))
    except Exception:
        return None


def send_one(sqs, queue_url, run_id, index, payload_bytes):
    start = now()
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=message_body(run_id, index, payload_bytes))
        return {
            "phase": "send",
            "index": index,
            "latency_s": now() - start,
            "messages": 1,
            "success": True,
            "error": "",
        }
    except Exception as exc:
        return {
            "phase": "send",
            "index": index,
            "latency_s": now() - start,
            "messages": 0,
            "success": False,
            "error": str(exc),
        }


def delete_batch(sqs, queue_url, messages):
    if not messages:
        return 0.0, 0, ""
    entries = [
        {"Id": str(index), "ReceiptHandle": msg["ReceiptHandle"]}
        for index, msg in enumerate(messages)
    ]
    start = now()
    try:
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        failed = response.get("Failed", [])
        if failed:
            return now() - start, len(messages) - len(failed), json.dumps(failed)
        return now() - start, len(messages), ""
    except Exception as exc:
        return now() - start, 0, str(exc)


def receive_delete_worker(sqs, queue_url, args, remaining):
    records = []
    while True:
        with remaining["lock"]:
            if remaining["count"] <= 0:
                return records
            requested = min(args.batch_size, remaining["count"])
            remaining["count"] -= requested

        receive_start = now()
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=requested,
                WaitTimeSeconds=args.wait_time_seconds,
                AttributeNames=["SentTimestamp"],
            )
            receive_latency = now() - receive_start
            messages = response.get("Messages", [])
            if not messages:
                with remaining["lock"]:
                    remaining["count"] += requested
                records.append(
                    {
                        "phase": "receive",
                        "index": -1,
                        "latency_s": receive_latency,
                        "messages": 0,
                        "queue_delay_s": "",
                        "success": True,
                        "error": "",
                    }
                )
                if args.stop_on_empty:
                    return records
                continue

            receive_time = wall_time()
            records.append(
                {
                    "phase": "receive",
                    "index": -1,
                    "latency_s": receive_latency,
                    "messages": len(messages),
                    "queue_delay_s": "",
                    "success": True,
                    "error": "",
                }
            )
            for msg in messages:
                sent_at = parse_sent_at(msg.get("Body", ""))
                if sent_at is None:
                    sent_ms = msg.get("Attributes", {}).get("SentTimestamp")
                    sent_at = float(sent_ms) / 1000 if sent_ms else None
                if sent_at:
                    records.append(
                        {
                            "phase": "queue_delay",
                            "index": -1,
                            "latency_s": receive_time - sent_at,
                            "messages": 1,
                            "queue_delay_s": receive_time - sent_at,
                            "success": True,
                            "error": "",
                        }
                    )

            if args.delete:
                delete_latency, deleted, error = delete_batch(sqs, queue_url, messages)
                records.append(
                    {
                        "phase": "delete",
                        "index": -1,
                        "latency_s": delete_latency,
                        "messages": deleted,
                        "queue_delay_s": "",
                        "success": not error,
                        "error": error,
                    }
                )
        except Exception as exc:
            with remaining["lock"]:
                remaining["count"] += requested
            records.append(
                {
                    "phase": "receive",
                    "index": -1,
                    "latency_s": now() - receive_start,
                    "messages": 0,
                    "queue_delay_s": "",
                    "success": False,
                    "error": str(exc),
                }
            )
            return records


def summarize(args, queue_url, records, elapsed_s):
    successes = [record for record in records if record["success"]]
    failures = [record for record in records if not record["success"]]
    send_messages = sum(record["messages"] for record in successes if record["phase"] == "send")
    receive_messages = sum(record["messages"] for record in successes if record["phase"] == "receive")
    delete_messages = sum(record["messages"] for record in successes if record["phase"] == "delete")
    if args.mode == "send":
        throughput_messages = send_messages
    elif args.delete:
        throughput_messages = delete_messages
    else:
        throughput_messages = receive_messages
    summary = {
        "queue_backend": args.queue_backend,
        "endpoint_url": args.endpoint_url,
        "queue_url": queue_url,
        "mode": args.mode,
        "messages": args.messages,
        "payload_bytes": args.payload_bytes,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
        "wait_time_seconds": args.wait_time_seconds,
        "delete": args.delete,
        "elapsed_s": elapsed_s,
        "success_records": len(successes),
        "failures": len(failures),
        "throughput_messages_s": throughput_messages / elapsed_s if elapsed_s > 0 else 0,
        "by_phase": {},
    }
    for phase in sorted({record["phase"] for record in records}):
        phase_records = [record for record in successes if record["phase"] == phase]
        summary["by_phase"][phase] = {
            "records": len(phase_records),
            "messages": sum(record["messages"] for record in phase_records),
            "latency_s": metrics([record["latency_s"] for record in phase_records]),
        }
        delays = [
            record.get("queue_delay_s")
            for record in phase_records
            if record.get("queue_delay_s") not in ("", None)
        ]
        if delays:
            summary["by_phase"][phase]["queue_delay_s"] = metrics(delays)
        if phase == "receive":
            received = sum(record["messages"] for record in phase_records)
            requested = len(phase_records) * args.batch_size
            summary["by_phase"][phase]["batch_fill_ratio"] = received / requested if requested else 0
    if failures:
        summary["errors"] = failures[:10]
    return summary


def write_csv(path, records):
    if not path:
        return
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["phase", "index", "latency_s", "messages", "queue_delay_s", "success", "error"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            for field in fieldnames:
                record.setdefault(field, "")
            writer.writerow(record)


def main():
    parser = argparse.ArgumentParser(description="Microbenchmark SQS-compatible queue latency and throughput.")
    parser.add_argument("--env-file", default="deploy/exchange/.env", help="Optional env file with ElasticMQ/AWS SQS settings.")
    parser.add_argument("--queue-backend", choices=["elasticmq", "aws_sqs", "sqs"], default=None)
    parser.add_argument("--endpoint-url", default=None, help="SQS endpoint URL. Omit for AWS SQS.")
    parser.add_argument("--queue-url", default=None)
    parser.add_argument("--queue-name", default=None)
    parser.add_argument("--aws-account-id", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--session-token", default=None)
    parser.add_argument("--max-pool-connections", type=int, default=None)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--mode", choices=["send", "receive-delete", "roundtrip"], default="roundtrip")
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--wait-time-seconds", type=int, default=0)
    parser.add_argument("--no-delete", dest="delete", action="store_false")
    parser.add_argument("--stop-on-empty", action="store_true")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--records-csv", default="")
    args = parser.parse_args()

    load_env_file(args.env_file)
    args.queue_backend = args.queue_backend or os.getenv("QUEUE_BACKEND", "elasticmq")
    args.region = args.region or os.getenv("AWS_REGION", "us-east-1")
    args.aws_account_id = args.aws_account_id or os.getenv("AWS_ACCOUNT_ID")
    args.queue_url = args.queue_url or default_queue_url()
    args.queue_name = args.queue_name or default_queue_name()
    if args.endpoint_url is None:
        args.endpoint_url = None if is_aws_queue_backend(args.queue_backend) else os.getenv("SQS_ENDPOINT_URL") or os.getenv("ELASTICMQ_ENDPOINT")
    args.endpoint_url = args.endpoint_url or None
    if not is_aws_queue_backend(args.queue_backend):
        args.access_key = args.access_key or "x"
        args.secret_key = args.secret_key or "x"
    args.max_pool_connections = args.max_pool_connections or int(os.getenv("AWS_MAX_POOL_CONNECTIONS", "10"))
    args.batch_size = max(1, min(args.batch_size, 10))

    if args.messages <= 0 or args.concurrency <= 0:
        raise ValueError("--messages and --concurrency must be positive")
    if args.wait_time_seconds < 0 or args.wait_time_seconds > 20:
        raise ValueError("--wait-time-seconds must be between 0 and 20")

    sqs = make_sqs_client(args)
    queue_url = resolve_queue_url(sqs, args)
    run_id = uuid.uuid4().hex
    records = []

    print(
        "[sqs-bench] starting "
        f"backend={args.queue_backend} mode={args.mode} queue_url={queue_url} "
        f"messages={args.messages} concurrency={args.concurrency} batch_size={args.batch_size} "
        f"wait_time={args.wait_time_seconds}",
        flush=True,
    )

    elapsed_start = now()
    if args.mode in {"send", "roundtrip"}:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [
                pool.submit(send_one, sqs, queue_url, run_id, index, args.payload_bytes)
                for index in range(args.messages)
            ]
            for future in as_completed(futures):
                records.append(future.result())

    if args.mode in {"receive-delete", "roundtrip"}:
        import threading

        remaining = {"count": args.messages, "lock": threading.Lock()}
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [
                pool.submit(receive_delete_worker, sqs, queue_url, args, remaining)
                for _ in range(args.concurrency)
            ]
            for future in as_completed(futures):
                records.extend(future.result())
    elapsed_s = now() - elapsed_start

    summary = summarize(args, queue_url, records, elapsed_s)
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[sqs-bench] wrote summary {output_path}", flush=True)
    if args.records_csv:
        write_csv(args.records_csv, records)
        print(f"[sqs-bench] wrote records {args.records_csv}", flush=True)


if __name__ == "__main__":
    main()
