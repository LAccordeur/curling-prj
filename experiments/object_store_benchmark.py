#!/usr/bin/env python3
import argparse
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
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def parse_size(value):
    text = str(value).strip().lower()
    multipliers = {
        "b": 1,
        "kb": 1024,
        "k": 1024,
        "mb": 1024 * 1024,
        "m": 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "g": 1024 * 1024 * 1024,
    }
    for suffix, multiplier in sorted(multipliers.items(), key=lambda item: len(item[0]), reverse=True):
        if text.endswith(suffix):
            return int(float(text[: -len(suffix)]) * multiplier)
    return int(text)


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


def first_csv_value(value):
    if not value:
        return ""
    return next((item.strip() for item in value.split(",") if item.strip()), "")


def default_bucket():
    return (
        first_csv_value(os.getenv("PRIVATE_DOMAIN_STAGED_BUCKETS"))
        or os.getenv("S3_BUCKET_STAGED")
        or os.getenv("S3_BUCKET_RELEASED")
    )


def is_aws_s3_backend(backend):
    return str(backend or "").lower() in {"aws_s3", "s3", "aws"}


def make_payload(size_bytes):
    prefix = f"curling-object-store-benchmark-{uuid.uuid4()}-".encode("utf-8")
    if len(prefix) >= size_bytes:
        return prefix[:size_bytes]
    return prefix + (b"x" * (size_bytes - len(prefix)))


def drain_body(body):
    total = 0
    for chunk in body.iter_chunks(chunk_size=1024 * 1024):
        total += len(chunk)
    return total


def put_object(s3, bucket, key, payload):
    start = now()
    s3.put_object(Bucket=bucket, Key=key, Body=payload)
    return now() - start, len(payload)


def get_object(s3, bucket, key):
    start = now()
    obj = s3.get_object(Bucket=bucket, Key=key)
    bytes_read = drain_body(obj["Body"])
    return now() - start, bytes_read


def record_result(index, op, latency_s, bytes_count, error=""):
    return {
        "index": index,
        "op": op,
        "latency_s": latency_s,
        "bytes": bytes_count,
        "success": not error,
        "error": error,
    }


def run_task(index, op, s3, bucket, prefix, payload, get_keys):
    try:
        if op == "put":
            key = f"{prefix}/put-{index:06d}-{uuid.uuid4().hex}.bin"
            latency_s, bytes_count = put_object(s3, bucket, key, payload)
            return record_result(index, op, latency_s, bytes_count), key

        key = get_keys[index % len(get_keys)]
        latency_s, bytes_count = get_object(s3, bucket, key)
        return record_result(index, op, latency_s, bytes_count), None
    except Exception as exc:
        return record_result(index, op, 0, 0, str(exc)), None


def prepopulate_get_objects(s3, bucket, prefix, payload, count):
    keys = []
    for index in range(count):
        key = f"{prefix}/get-source-{index:06d}.bin"
        s3.put_object(Bucket=bucket, Key=key, Body=payload)
        keys.append(key)
    return keys


def cleanup_prefix(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def summarize(args, records, elapsed_s):
    successes = [record for record in records if record["success"]]
    failures = [record for record in records if not record["success"]]
    total_bytes = sum(record["bytes"] for record in successes)
    summary = {
        "op": args.op,
        "object_store_backend": args.object_store_backend,
        "endpoint_url": args.endpoint_url,
        "bucket": args.bucket,
        "prefix": args.prefix,
        "object_size_bytes": args.object_size,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "elapsed_s": elapsed_s,
        "successes": len(successes),
        "failures": len(failures),
        "throughput_ops_s": len(successes) / elapsed_s if elapsed_s > 0 else 0,
        "throughput_mb_s": (total_bytes / (1024 * 1024)) / elapsed_s if elapsed_s > 0 else 0,
        "latency_s": metrics([record["latency_s"] for record in successes]),
        "by_op": {},
    }
    for op in sorted({record["op"] for record in records}):
        op_records = [record for record in successes if record["op"] == op]
        op_bytes = sum(record["bytes"] for record in op_records)
        summary["by_op"][op] = {
            "count": len(op_records),
            "throughput_mb_s": (op_bytes / (1024 * 1024)) / elapsed_s if elapsed_s > 0 else 0,
            "latency_s": metrics([record["latency_s"] for record in op_records]),
        }
    if failures:
        summary["errors"] = [record for record in failures[:10]]
    return summary


def main():
    parser = argparse.ArgumentParser(description="Microbenchmark object-store PUT/GET latency and throughput.")
    parser.add_argument("--env-file", default="deploy/exchange/.env", help="Optional env file with MinIO/S3 settings.")
    parser.add_argument("--object-store-backend", default=None, choices=["minio", "aws_s3", "s3"], help="Object store backend. Defaults to OBJECT_STORE_BACKEND or minio.")
    parser.add_argument("--endpoint-url", default=None, help="S3 endpoint URL. Use empty string for AWS S3.")
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--access-key", default=None)
    parser.add_argument("--secret-key", default=None)
    parser.add_argument("--session-token", default=None)
    parser.add_argument("--max-pool-connections", type=int, default=None)
    parser.add_argument("--op", choices=["put", "get", "mixed"], default="put")
    parser.add_argument("--object-size", type=parse_size, default=parse_size("8MB"))
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--get-source-count", type=int, default=0, help="Objects to prepopulate for GET/mixed. Default: concurrency.")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--records-csv", default="")
    parser.add_argument("--no-cleanup", action="store_true")
    args = parser.parse_args()

    load_env_file(args.env_file)
    args.object_store_backend = args.object_store_backend or os.getenv("OBJECT_STORE_BACKEND", "minio")
    if args.endpoint_url is None:
        args.endpoint_url = ""
        if not is_aws_s3_backend(args.object_store_backend):
            args.endpoint_url = os.getenv("S3_ENDPOINT_URL") or os.getenv("MINIO_ENDPOINT") or ""
    args.endpoint_url = args.endpoint_url or None
    args.bucket = args.bucket or default_bucket()
    args.region = args.region or os.getenv("AWS_REGION", "us-east-1")
    if not is_aws_s3_backend(args.object_store_backend):
        args.access_key = args.access_key or os.getenv("MINIO_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER", "x")
        args.secret_key = args.secret_key or os.getenv("MINIO_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD", "x")
    args.max_pool_connections = args.max_pool_connections or int(os.getenv("AWS_MAX_POOL_CONNECTIONS", "10"))
    args.prefix = args.prefix or f"object-benchmark/{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:8]}"

    if not args.bucket:
        raise ValueError("bucket is required; pass --bucket or set S3_BUCKET_STAGED/PRIVATE_DOMAIN_STAGED_BUCKETS")
    if args.requests <= 0 or args.concurrency <= 0:
        raise ValueError("--requests and --concurrency must be positive")

    cfg_kwargs = {
        "region_name": args.region,
        "max_pool_connections": args.max_pool_connections,
        "retries": {"mode": "standard", "total_max_attempts": 3},
    }
    if not is_aws_s3_backend(args.object_store_backend):
        cfg_kwargs["s3"] = {"addressing_style": "path"}
    cfg = Config(**cfg_kwargs)
    client_kwargs = {"endpoint_url": args.endpoint_url, "config": cfg}
    if args.access_key and args.secret_key:
        client_kwargs["aws_access_key_id"] = args.access_key
        client_kwargs["aws_secret_access_key"] = args.secret_key
        if args.session_token:
            client_kwargs["aws_session_token"] = args.session_token
    s3 = boto3.client("s3", **client_kwargs)
    payload = make_payload(args.object_size)

    get_keys = []
    if args.op in {"get", "mixed"}:
        source_count = args.get_source_count or args.concurrency
        print(f"[object-bench] prepopulating get objects count={source_count} bucket={args.bucket} prefix={args.prefix}", flush=True)
        get_keys = prepopulate_get_objects(s3, args.bucket, args.prefix, payload, source_count)

    print(
        "[object-bench] starting "
        f"backend={args.object_store_backend} op={args.op} bucket={args.bucket} object_size={args.object_size} "
        f"requests={args.requests} concurrency={args.concurrency} "
        f"max_pool_connections={args.max_pool_connections}",
        flush=True,
    )

    records = []
    put_keys = []
    elapsed_start = now()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = []
        for index in range(args.requests):
            op = args.op
            if args.op == "mixed":
                op = "put" if index % 2 == 0 else "get"
            futures.append(pool.submit(run_task, index, op, s3, args.bucket, args.prefix, payload, get_keys))
        for future in as_completed(futures):
            record, put_key = future.result()
            records.append(record)
            if put_key:
                put_keys.append(put_key)
    elapsed_s = now() - elapsed_start

    summary = summarize(args, records, elapsed_s)
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[object-bench] wrote summary {output_path}", flush=True)

    if args.records_csv:
        import csv

        csv_path = Path(args.records_csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["index", "op", "latency_s", "bytes", "success", "error"])
            writer.writeheader()
            for record in sorted(records, key=lambda item: item["index"]):
                writer.writerow(record)
        print(f"[object-bench] wrote records {csv_path}", flush=True)

    if not args.no_cleanup:
        print(f"[object-bench] cleaning up prefix s3://{args.bucket}/{args.prefix}/", flush=True)
        cleanup_prefix(s3, args.bucket, args.prefix)


if __name__ == "__main__":
    main()
