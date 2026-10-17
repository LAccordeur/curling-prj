import json
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from threading import Lock

import boto3
from queue_diagnostics import QueueDiagnostics
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text

from config import (
    OBJECT_STORE_BACKEND,
    QUEUE_BACKEND,
    MINIO_ACCESS_KEY_ID,
    MINIO_SECRET_ACCESS_KEY,
    AWS_MAX_POOL_CONNECTIONS,
    AWS_REGION,
    DB_MAX_OVERFLOW,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT,
    DB_URL,
    EXECUTOR_LOG_ENABLED,
    EXECUTOR_DIAGNOSTICS_ENABLED,
    EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS,
    EXECUTOR_INFLIGHT_REUSE_ENABLED,
    EXECUTOR_MAX_BACKOFF_SECONDS,
    EXECUTOR_MIN_BACKOFF_SECONDS,
    EXECUTOR_NOOP_ENABLED,
    EXECUTOR_SCHEDULER_BATCH_SIZE,
    EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS,
    EXECUTOR_SCHEDULER_MODE,
    EXECUTOR_STAGING_ENABLED,
    EXECUTOR_WORKER_COUNT,
    PRIVATE_BASE,
    PRIVATE_DOMAIN_ID,
    S3_BUCKET_RELEASED,
    S3_BUCKET_STAGED,
    S3_ENDPOINT_URL,
    SQS_ENDPOINT_URL,
    SQS_QUEUE_URL,
    WORK_BASE,
)

engine = create_engine(
    DB_URL,
    future=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
)
cfg = Config(
    region_name=AWS_REGION,
    max_pool_connections=AWS_MAX_POOL_CONNECTIONS,
    retries={"mode": "standard", "total_max_attempts": 5},
)

def boto3_client(service_name, endpoint_url=None):
    kwargs = {"endpoint_url": endpoint_url or None, "config": cfg}
    if service_name == "s3" and OBJECT_STORE_BACKEND == "minio":
        if not endpoint_url or not MINIO_ACCESS_KEY_ID or not MINIO_SECRET_ACCESS_KEY:
            raise ValueError("MinIO requires S3_ENDPOINT_URL and MINIO_ACCESS_KEY_ID/MINIO_SECRET_ACCESS_KEY; regenerate env files")
        kwargs["aws_access_key_id"] = MINIO_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = MINIO_SECRET_ACCESS_KEY
    elif service_name == "sqs" and QUEUE_BACKEND == "elasticmq":
        if not endpoint_url:
            raise ValueError("ElasticMQ requires SQS_ENDPOINT_URL")
        kwargs["aws_access_key_id"] = "x"
        kwargs["aws_secret_access_key"] = "x"
    # AWS clients use the provider chain, including profile/session-token credentials.
    return boto3.client(service_name, **kwargs)

s3 = boto3_client("s3", S3_ENDPOINT_URL)
sqs = boto3_client("sqs", SQS_ENDPOINT_URL)
diagnostics = QueueDiagnostics(EXECUTOR_DIAGNOSTICS_ENABLED, EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS)

MIN_BACKOFF_SECONDS = EXECUTOR_MIN_BACKOFF_SECONDS
MAX_BACKOFF_SECONDS = EXECUTOR_MAX_BACKOFF_SECONDS
POLL_BATCH_SIZE = 10
LONG_POLL_WAIT_SECONDS = 0
POLL_IDLE_SLEEP_SECONDS = 0.01
SCHEDULER_BATCH_SIZE = EXECUTOR_SCHEDULER_BATCH_SIZE
SCHEDULER_BATCH_WAIT_SECONDS = EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS
TERMINAL_STATES = {"RELEASED", "COMMITTED", "FAILED", "REJECTED"}
REUSE_COUNTER_LOCK = Lock()
REUSE_COUNTER = {
    "total": 0,
    "artifact": 0,
    "intra_batch": 0,
    "inflight": 0,
}
REUSE_KEY_LOCKS_LOCK = Lock()
REUSE_KEY_LOCKS = {}

def msg_domain_id(msg):
    return msg.get("domain_id") or PRIVATE_DOMAIN_ID

def log(message):
    if EXECUTOR_LOG_ENABLED:
        print(f"[executor] {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {message}", flush=True)

def increment_reuse_counter(source, count=1):
    if count <= 0:
        return
    with REUSE_COUNTER_LOCK:
        REUSE_COUNTER["total"] += count
        REUSE_COUNTER[source] = REUSE_COUNTER.get(source, 0) + count
        total = REUSE_COUNTER["total"]
        source_total = REUSE_COUNTER[source]
        artifact_total = REUSE_COUNTER.get("artifact", 0)
        intra_batch_total = REUSE_COUNTER.get("intra_batch", 0)
        inflight_total = REUSE_COUNTER.get("inflight", 0)
    log(
        "reuse counter "
        f"source={source} increment={count} "
        f"source_total={source_total} total={total} "
        f"artifact_total={artifact_total} "
        f"intra_batch_total={intra_batch_total} "
        f"inflight_total={inflight_total}"
    )

def acquire_reuse_key_lock(key):
    if key is None:
        return None, 0.0
    with REUSE_KEY_LOCKS_LOCK:
        entry = REUSE_KEY_LOCKS.get(key)
        if entry is None:
            entry = {"lock": Lock(), "refs": 0}
            REUSE_KEY_LOCKS[key] = entry
        entry["refs"] += 1
    wait_start = now()
    entry["lock"].acquire()
    return entry, now() - wait_start

def release_reuse_key_lock(key, entry):
    if key is None or entry is None:
        return
    entry["lock"].release()
    with REUSE_KEY_LOCKS_LOCK:
        entry["refs"] -= 1
        if entry["refs"] == 0 and REUSE_KEY_LOCKS.get(key) is entry:
            del REUSE_KEY_LOCKS[key]

def now():
    return time.perf_counter()

def log_timings(op, request_id, timings, total_start, **fields):
    parts = [f"{name}={value:.6f}s" for name, value in timings.items()]
    parts.append(f"total={now() - total_start:.6f}s")
    for name, value in fields.items():
        parts.append(f"{name}={value}")
    log(f"{op} timings request_id={request_id} " + " ".join(parts))

def set_state(request_id, state):
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE requests
                SET state=:state, updated_at=NOW()
                WHERE request_id=:id
                  AND (
                    state NOT IN ('RELEASED', 'COMMITTED', 'FAILED', 'REJECTED')
                    OR state=:state
                  )
            """),
            {"state": state, "id": request_id},
        )
        if result.rowcount:
            log(f"request_id={request_id} state={state}")
            return True

        current_state = conn.execute(
            text("SELECT state FROM requests WHERE request_id=:id"),
            {"id": request_id},
        ).scalar_one_or_none()
    log(f"request_id={request_id} state_transition_skipped target={state} current={current_state}")
    return False

def release_request_with_artifact(request_id, artifact_id, s3_key, domain_id=PRIVATE_DOMAIN_ID):
    with engine.begin() as conn:
        current_state = conn.execute(
            text("SELECT state FROM requests WHERE request_id=:id AND domain_id=:domain_id FOR UPDATE"),
            {"id": request_id, "domain_id": domain_id},
        ).scalar_one_or_none()
        if current_state in TERMINAL_STATES:
            log(
                "release skipped terminal request "
                f"request_id={request_id} current={current_state} artifact_id={artifact_id}"
            )
            return False
        if current_state is None:
            log(f"release skipped missing request request_id={request_id} artifact_id={artifact_id}")
            return False

        conn.execute(
            text("""
                INSERT INTO artifacts(artifact_id, domain_id, request_id, s3_key, state)
                VALUES (:artifact_id, :domain_id, :request_id, :s3_key, 'READY')
            """),
            {"artifact_id": artifact_id, "domain_id": domain_id, "request_id": request_id, "s3_key": s3_key},
        )
        conn.execute(
            text("UPDATE requests SET state='RELEASED', updated_at=NOW() WHERE request_id=:id AND domain_id=:domain_id"),
            {"id": request_id, "domain_id": domain_id},
        )
    log(f"request_id={request_id} domain_id={domain_id} state=RELEASED artifact_id={artifact_id}")
    return True

def handle_noop(msg):
    total_start = now()
    timings = {}
    request_id = msg["request_id"]
    request_type = msg.get("type")
    if request_type == "read":
        final_state = "RELEASED"
    elif request_type == "write":
        final_state = "COMMITTED"
    else:
        log(f"noop unknown command type request_id={request_id} type={request_type}")
        set_state(request_id, "FAILED")
        return False

    log(f"noop start request_id={request_id} type={request_type}")
    t0 = now()
    if not set_state(request_id, "PROCESSING"):
        timings["state_processing"] = now() - t0
        log_timings(
            "noop",
            request_id,
            timings,
            total_start,
            status="skipped_terminal",
            type=request_type,
            queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
        )
        return False
    timings["state_processing"] = now() - t0
    t0 = now()
    set_state(request_id, final_state)
    timings["state_final"] = now() - t0
    log_timings(
        "noop",
        request_id,
        timings,
        total_start,
        status=final_state.lower(),
        type=request_type,
        queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
    )
    return True

def process_read_data(data):
    return data

def process_write_data(data):
    return data

def ensure_bytes(data):
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode("utf-8")
    return bytes(data)

def handle_read(msg):
    total_start = now()
    timings = {}
    request_id = msg["request_id"]
    domain_id = msg_domain_id(msg)
    target = msg["target_spec"]
    log(f"read start request_id={request_id} domain_id={domain_id} target={target}")
    t0 = now()
    if not set_state(request_id, "PROCESSING"):
        timings["state_processing"] = now() - t0
        log_timings(
            "read",
            request_id,
            timings,
            total_start,
            status="skipped_terminal",
            domain_id=domain_id,
            target=target,
            queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
        )
        return None
    timings["state_processing"] = now() - t0

    src = PRIVATE_BASE / "exports" / f"{target}.txt"
    if not src.exists():
        log(f"read missing source request_id={request_id} path={src}")
        t0 = now()
        set_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings("read", request_id, timings, total_start, status="failed_missing_source", target=target)
        return None
    artifact_id = str(uuid.uuid4())

    t0 = now()
    raw_data = src.read_bytes()
    timings["private_read"] = now() - t0

    t0 = now()
    processed_data = ensure_bytes(process_read_data(raw_data))
    timings["process"] = now() - t0
    if not processed_data:
        log(f"read rejected empty processed data request_id={request_id}")
        t0 = now()
        set_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "read",
            request_id,
            timings,
            total_start,
            status="failed_empty_processed_data",
            target=target,
            bytes_in=len(raw_data),
        )
        return None
    s3_key = f"released/{request_id}/{artifact_id}.txt"
    if EXECUTOR_STAGING_ENABLED:
        out_dir = WORK_BASE / "outbound"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{artifact_id}.txt"
        t0 = now()
        out.write_bytes(processed_data)
        timings["stage_write"] = now() - t0
        log(f"read processed request_id={request_id} output={out}")
        t0 = now()
        s3.upload_file(str(out), S3_BUCKET_RELEASED, s3_key)
        timings["s3_upload"] = now() - t0
    else:
        log(f"read processed request_id={request_id} bytes={len(processed_data)}")
        t0 = now()
        s3.put_object(Bucket=S3_BUCKET_RELEASED, Key=s3_key, Body=processed_data)
        timings["s3_upload"] = now() - t0
    log(f"read uploaded request_id={request_id} bucket={S3_BUCKET_RELEASED} key={s3_key}")
    t0 = now()
    if not release_request_with_artifact(request_id, artifact_id, s3_key, domain_id):
        timings["db_release_artifact"] = now() - t0
        log_timings(
            "read",
            request_id,
            timings,
            total_start,
            status="skipped_terminal_after_upload",
            domain_id=domain_id,
            target=target,
            bytes_in=len(raw_data),
            bytes_out=len(processed_data),
            staging=EXECUTOR_STAGING_ENABLED,
            queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
        )
        return None
    timings["db_release_artifact"] = now() - t0
    log(f"read released request_id={request_id} artifact_id={artifact_id}")
    log_timings(
        "read",
        request_id,
        timings,
        total_start,
        status="released",
        domain_id=domain_id,
        target=target,
        bytes_in=len(raw_data),
        bytes_out=len(processed_data),
        staging=EXECUTOR_STAGING_ENABLED,
        queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
    )
    return {"artifact_id": artifact_id, "s3_key": s3_key}

def handle_write(msg):
    total_start = now()
    timings = {}
    request_id = msg["request_id"]
    domain_id = msg_domain_id(msg)
    upload_id = msg["upload_id"]
    target = msg["target_spec"]
    log(f"write start request_id={request_id} domain_id={domain_id} upload_id={upload_id} target={target}")
    t0 = now()
    if not set_state(request_id, "PROCESSING"):
        timings["state_processing"] = now() - t0
        log_timings(
            "write",
            request_id,
            timings,
            total_start,
            status="skipped_terminal",
            domain_id=domain_id,
            upload_id=upload_id,
            target=target,
            queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
        )
        return False
    timings["state_processing"] = now() - t0
    t0 = now()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT s3_key FROM staged_uploads WHERE upload_id=:id AND domain_id=:domain_id"),
            {"id": upload_id, "domain_id": domain_id},
        ).mappings().first()
    timings["db_lookup_staged_upload"] = now() - t0
    if not row:
        log(f"write missing staged_upload request_id={request_id} upload_id={upload_id}")
        t0 = now()
        set_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings("write", request_id, timings, total_start, status="failed_missing_staged_upload", domain_id=domain_id, upload_id=upload_id, target=target)
        return False
    s3_key = row["s3_key"]
    try:
        t0 = now()
        s3.head_object(Bucket=S3_BUCKET_STAGED, Key=s3_key)
        timings["s3_head_staged"] = now() - t0
    except ClientError:
        timings["s3_head_staged"] = now() - t0
        log(f"write missing staged object request_id={request_id} bucket={S3_BUCKET_STAGED} key={s3_key}")
        t0 = now()
        set_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings("write", request_id, timings, total_start, status="failed_missing_staged_object", domain_id=domain_id, upload_id=upload_id, target=target)
        return False

    s3_retry_attempts = "n/a"
    if EXECUTOR_STAGING_ENABLED:
        in_dir = WORK_BASE / "inbound"
        in_dir.mkdir(parents=True, exist_ok=True)
        local_path = in_dir / f"{upload_id}.file"
        t0 = now()
        s3.download_file(S3_BUCKET_STAGED, s3_key, str(local_path))
        timings["s3_download"] = now() - t0
        log(f"write downloaded request_id={request_id} local_path={local_path}")
        t0 = now()
        raw_data = local_path.read_bytes()
        timings["stage_read"] = now() - t0
    else:
        download_start = now()
        obj = s3.get_object(Bucket=S3_BUCKET_STAGED, Key=s3_key)
        timings["s3_get_headers"] = now() - download_start
        s3_retry_attempts = obj.get("ResponseMetadata", {}).get("RetryAttempts", "n/a")
        body = obj["Body"]
        t0 = now()
        try:
            raw_data = body.read()
        finally:
            timings["s3_body_read"] = now() - t0
            body.close()
        timings["s3_download"] = now() - download_start
        log(f"write downloaded request_id={request_id} bytes={len(raw_data)}")
    t0 = now()
    processed_data = ensure_bytes(process_write_data(raw_data))
    timings["process"] = now() - t0
    if not processed_data:
        log(f"write rejected empty processed data request_id={request_id}")
        t0 = now()
        set_state(request_id, "REJECTED")
        timings["state_rejected"] = now() - t0
        log_timings(
            "write",
            request_id,
            timings,
            total_start,
            status="rejected_empty_processed_data",
            s3_retry_attempts=s3_retry_attempts,
            domain_id=domain_id,
            upload_id=upload_id,
            target=target,
            bytes_in=len(raw_data),
        )
        return False
    dst_dir = PRIVATE_BASE / "exports"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / f"{target}_{upload_id}.file"
    t0 = now()
    dst.write_bytes(processed_data)
    timings["private_write"] = now() - t0
    log(f"write committed data request_id={request_id} path={dst}")
    t0 = now()
    set_state(request_id, "COMMITTED")
    timings["state_committed"] = now() - t0
    log_timings(
        "write",
        request_id,
        timings,
        total_start,
        status="committed",
        s3_retry_attempts=s3_retry_attempts,
        domain_id=domain_id,
        upload_id=upload_id,
        target=target,
        bytes_in=len(raw_data),
        bytes_out=len(processed_data),
        staging=EXECUTOR_STAGING_ENABLED,
        queue_delay_s=fmt_optional_seconds(msg.get("_queue_delay_s")),
    )
    return True

def parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() not in {"0", "false", "no", "off"}
    return bool(value)

def fmt_optional_seconds(value):
    if value is None:
        return "n/a"
    return f"{value:.6f}s"

def parse_cmd(message, received_at_epoch_s=None):
    try:
        body = json.loads(message["Body"])
    except json.JSONDecodeError:
        log(f"failed to parse JSON body={message['Body']}")
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
        return None
    body["_receipt_handle"] = message["ReceiptHandle"]
    if diagnostics.enabled:
        body.update({key: value for key, value in message.items() if key.startswith("_diag_")})
        body["_diag_message_id"] = message.get("MessageId")
        attributes = message.get("Attributes", {})
        diagnostics.emit(
            "message_received", request_id=body.get("request_id"), message_id=message.get("MessageId"),
            receive_id=message.get("_diag_receive_id"), received_at_epoch_s=received_at_epoch_s,
            sent_timestamp_ms=attributes.get("SentTimestamp"),
            first_receive_timestamp_ms=attributes.get("ApproximateFirstReceiveTimestamp"),
            receive_count=attributes.get("ApproximateReceiveCount"),
        )
    body["domain_id"] = body.get("domain_id") or PRIVATE_DOMAIN_ID
    body["priority"] = int(body.get("priority", 0))
    body["reuse_enabled"] = parse_bool(body.get("reuse_enabled", True))
    sent_timestamp_ms = message.get("Attributes", {}).get("SentTimestamp")
    enqueued_at = float(sent_timestamp_ms) / 1000 if sent_timestamp_ms else body.get("_enqueued_at_epoch_s")
    if enqueued_at is not None and received_at_epoch_s is not None:
        try:
            body["_queue_delay_s"] = max(0.0, received_at_epoch_s - float(enqueued_at))
            log(
                "queue delay "
                f"request_id={body.get('request_id')} "
                f"type={body.get('type')} "
                f"source={'sqs_sent_timestamp' if sent_timestamp_ms else 'api_enqueued_at'} "
                f"delay={body['_queue_delay_s']:.6f}s"
            )
        except (TypeError, ValueError):
            log(f"queue delay unavailable request_id={body.get('request_id')} invalid_enqueued_at={enqueued_at}")
    return body

def delete_cmd(cmd):
    t0 = now()
    try:
        response = sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=cmd["_receipt_handle"])
    except Exception as exc:
        diagnostics.add(delete_errors=1)
        diagnostics.emit("delete_error", request_id=cmd.get("request_id"),
                         elapsed_s=now() - t0, error=type(exc).__name__)
        raise
    diagnostics.add(deleted_messages=1, delete_time_s=now() - t0)
    metadata = response.get("ResponseMetadata", {}) if response else {}
    diagnostics.emit("delete_end", request_id=cmd.get("request_id"), elapsed_s=now() - t0,
                     retries=metadata.get("RetryAttempts"), aws_request_id=metadata.get("RequestId"))
    log(f"deleted queue message request_id={cmd.get('request_id')} sqs_delete_time={now() - t0:.6f}s")

def reuse_key(cmd):
    if cmd.get("type") != "read":
        return None
    return (msg_domain_id(cmd), cmd.get("target_spec"))

def artifact_is_reusable(artifact):
    return artifact is not None

def refresh_artifact_lifetime(artifact):
    pass

def find_reusable_artifact(cmd):
    if cmd.get("type") != "read" or not cmd.get("reuse_enabled", True):
        log(f"reuse skipped request_id={cmd.get('request_id')} type={cmd.get('type')}")
        return None
    domain_id = msg_domain_id(cmd)
    t0 = now()
    with engine.begin() as conn:
        artifact = conn.execute(
            text("""
                SELECT a.artifact_id, a.s3_key
                FROM artifacts a
                JOIN requests r ON r.request_id = a.request_id
                WHERE r.request_type = 'read'
                  AND r.domain_id = :domain_id
                  AND a.domain_id = :domain_id
                  AND r.target_spec = :target_spec
                  AND r.state = 'RELEASED'
                  AND a.state = 'READY'
                ORDER BY a.created_at DESC
                LIMIT 1
            """),
            {"domain_id": domain_id, "target_spec": cmd["target_spec"]},
        ).mappings().first()
    lookup_time = now() - t0
    if artifact:
        log(
            f"reuse hit request_id={cmd['request_id']} domain_id={domain_id} "
            f"target={cmd['target_spec']} artifact_id={artifact['artifact_id']} "
            f"reuse_lookup_time={lookup_time:.6f}s"
        )
    else:
        log(
            f"reuse miss request_id={cmd['request_id']} domain_id={domain_id} "
            f"target={cmd['target_spec']} reuse_lookup_time={lookup_time:.6f}s"
        )
    return artifact

def bind_request_to_artifact(cmd, artifact, reuse_source=None):
    artifact_id = str(uuid.uuid4())
    domain_id = msg_domain_id(cmd)
    t0 = now()
    if release_request_with_artifact(cmd["request_id"], artifact_id, artifact["s3_key"], domain_id):
        bind_time = now() - t0
        log(
            f"bound artifact request_id={cmd['request_id']} source_key={artifact['s3_key']} "
            f"new_artifact_id={artifact_id} reuse_source={reuse_source or 'unknown'} "
            f"reuse_bind_time={bind_time:.6f}s"
        )
        if reuse_source:
            increment_reuse_counter(reuse_source)
        return True
    bind_time = now() - t0
    log(
        f"bound artifact skipped request_id={cmd['request_id']} source_key={artifact['s3_key']} "
        f"new_artifact_id={artifact_id} reuse_source={reuse_source or 'unknown'} "
        f"reuse_bind_time={bind_time:.6f}s"
    )
    return False

def make_execution_units(cmds):
    units = []
    read_groups = {}
    for cmd in cmds:
        if cmd.get("type") == "write":
            units.append({"type": "write", "key": cmd["request_id"], "cmds": [cmd]})
        elif cmd.get("type") == "read":
            key = reuse_key(cmd)
            read_groups.setdefault(key, []).append(cmd)
    for key, group in read_groups.items():
        units.append({"type": "read", "key": key, "cmds": group})
    log(f"created execution_units count={len(units)} requests={len(cmds)}")
    return units

def scheduling_score(unit):
    return max(cmd.get("priority", 0) for cmd in unit["cmds"])

def resources_available(unit):
    return True

def within_tenant_budget(unit):
    return True

def execute_read_unit(unit):
    unit_start = now()
    primary = unit["cmds"][0]
    if not EXECUTOR_INFLIGHT_REUSE_ENABLED:
        artifact = handle_read(primary)
        if not artifact:
            for cmd in unit["cmds"][1:]:
                set_state(cmd["request_id"], "FAILED")
            return False

        intra_batch_reuse_bound = 0
        bind_total_time = 0.0
        for cmd in unit["cmds"][1:]:
            bind_start = now()
            if bind_request_to_artifact(cmd, artifact, reuse_source="intra_batch"):
                intra_batch_reuse_bound += 1
            bind_total_time += now() - bind_start
        log(
            "inflight_reuse_disabled "
            f"domain_id={msg_domain_id(primary)} "
            f"target={primary.get('target_spec')} "
            f"primary_request_id={primary.get('request_id')} "
            f"candidate_reuse_requests={len(unit['cmds']) - 1} "
            f"bound_reuse_requests={intra_batch_reuse_bound} "
            f"reuse_bind_total_time={bind_total_time:.6f}s "
            f"reuse_unit_total_time={now() - unit_start:.6f}s"
        )
        return True

    key = unit.get("key")
    lock_entry, lock_wait_time = acquire_reuse_key_lock(key)
    lock_hold_start = now()
    if lock_entry is not None:
        log(
            f"reuse key locked key={key} requests={len(unit['cmds'])} "
            f"reuse_lock_wait_time={lock_wait_time:.6f}s"
        )
    try:
        lookup_start = now()
        artifact = find_reusable_artifact(primary)
        initial_lookup_time = now() - lookup_start
        if artifact_is_reusable(artifact):
            refresh_artifact_lifetime(artifact)
            inflight_reuse_bound = 0
            bind_total_time = 0.0
            for cmd in unit["cmds"]:
                bind_start = now()
                if bind_request_to_artifact(cmd, artifact, reuse_source="inflight"):
                    inflight_reuse_bound += 1
                bind_total_time += now() - bind_start
            log(
                "inflight_reuse "
                f"domain_id={msg_domain_id(primary)} "
                f"target={primary.get('target_spec')} "
                f"requests={len(unit['cmds'])} "
                f"bound_reuse_requests={inflight_reuse_bound} "
                f"source_artifact_id={artifact['artifact_id']} "
                f"reuse_lock_wait_time={lock_wait_time:.6f}s "
                f"reuse_lock_hold_time={now() - lock_hold_start:.6f}s "
                f"reuse_initial_lookup_time={initial_lookup_time:.6f}s "
                f"reuse_bind_total_time={bind_total_time:.6f}s "
                f"reuse_unit_total_time={now() - unit_start:.6f}s"
            )
            return inflight_reuse_bound > 0

        read_start = now()
        artifact = handle_read(primary)
        primary_read_time = now() - read_start
        if not artifact:
            for cmd in unit["cmds"][1:]:
                set_state(cmd["request_id"], "FAILED")
            log(
                "reuse unit failed "
                f"key={key} primary_request_id={primary.get('request_id')} "
                f"reuse_lock_wait_time={lock_wait_time:.6f}s "
                f"reuse_lock_hold_time={now() - lock_hold_start:.6f}s "
                f"reuse_initial_lookup_time={initial_lookup_time:.6f}s "
                f"primary_read_time={primary_read_time:.6f}s "
                f"reuse_unit_total_time={now() - unit_start:.6f}s"
            )
            return False

        intra_batch_reuse_bound = 0
        bind_total_time = 0.0
        for cmd in unit["cmds"][1:]:
            bind_start = now()
            if bind_request_to_artifact(cmd, artifact, reuse_source="intra_batch"):
                intra_batch_reuse_bound += 1
            bind_total_time += now() - bind_start
        if len(unit["cmds"]) > 1:
            log(
                "intra_batch_reuse "
                f"domain_id={msg_domain_id(primary)} "
                f"target={primary.get('target_spec')} "
                f"primary_request_id={primary.get('request_id')} "
                f"candidate_reuse_requests={len(unit['cmds']) - 1} "
                f"bound_reuse_requests={intra_batch_reuse_bound} "
                f"reuse_lock_wait_time={lock_wait_time:.6f}s "
                f"reuse_lock_hold_time={now() - lock_hold_start:.6f}s "
                f"reuse_initial_lookup_time={initial_lookup_time:.6f}s "
                f"primary_read_time={primary_read_time:.6f}s "
                f"reuse_bind_total_time={bind_total_time:.6f}s "
                f"reuse_unit_total_time={now() - unit_start:.6f}s"
            )
        else:
            log(
                "reuse unit single "
                f"domain_id={msg_domain_id(primary)} "
                f"target={primary.get('target_spec')} "
                f"primary_request_id={primary.get('request_id')} "
                f"reuse_lock_wait_time={lock_wait_time:.6f}s "
                f"reuse_lock_hold_time={now() - lock_hold_start:.6f}s "
                f"reuse_initial_lookup_time={initial_lookup_time:.6f}s "
                f"primary_read_time={primary_read_time:.6f}s "
                f"reuse_unit_total_time={now() - unit_start:.6f}s"
            )
        return True
    finally:
        release_reuse_key_lock(key, lock_entry)

@diagnostics.execution("active_units", lambda unit: unit["cmds"])
def execute_unit(unit):
    log(f"execute unit type={unit['type']} key={unit['key']} requests={len(unit['cmds'])}")
    if EXECUTOR_NOOP_ENABLED:
        return all(handle_noop(cmd) for cmd in unit["cmds"])

    if unit["type"] == "write":
        return handle_write(unit["cmds"][0])

    return execute_read_unit(unit)

def fail_unit(unit):
    for cmd in unit["cmds"]:
        request_id = cmd.get("request_id")
        if request_id:
            set_state(request_id, "FAILED")

def delete_unit_cmds(unit):
    for cmd in unit["cmds"]:
        delete_cmd(cmd)

def execute_and_delete_unit(unit):
    try:
        return execute_unit(unit)
    except Exception as exc:
        log(f"unit failed type={unit['type']} key={unit['key']} error={exc}")
        fail_unit(unit)
        return False
    finally:
        delete_unit_cmds(unit)

@diagnostics.execution("active_reuse_hit_tasks", lambda cmd, artifact: [cmd])
def bind_and_delete_reuse_hit(cmd, artifact):
    total_start = now()
    refresh_artifact_lifetime(artifact)
    bound = bind_request_to_artifact(cmd, artifact, reuse_source="artifact")
    delete_cmd(cmd)
    log(
        "artifact_reuse_hit_processed "
        f"request_id={cmd.get('request_id')} "
        f"bound={bound} "
        f"total={now() - total_start:.6f}s"
    )
    return bound

def process_reuse_hits(reuse_hits, worker_count):
    if not reuse_hits:
        return 0
    workers = min(max(1, worker_count), len(reuse_hits))
    log(f"artifact reuse hits processing hits={len(reuse_hits)} workers={workers}")
    if workers <= 1:
        return sum(1 for cmd, artifact in reuse_hits if bind_and_delete_reuse_hit(cmd, artifact))

    bound_count = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(bind_and_delete_reuse_hit, cmd, artifact)
            for cmd, artifact in reuse_hits
        ]
        for future in as_completed(futures):
            if future.result():
                bound_count += 1
    return bound_count

def execute_batch(cmds, worker_count=None):
    worker_count = worker_count or EXECUTOR_WORKER_COUNT
    log(f"batch received messages={len(cmds)}")
    parsed_cmds = []
    for message in cmds:
        cmd = parse_cmd(message, message.get("_received_at_epoch_s"))
        if cmd:
            parsed_cmds.append(cmd)

    if EXECUTOR_NOOP_ENABLED:
        pending_cmds = parsed_cmds
        reuse_stats = None
    else:
        pending_cmds = []
        reuse_hits = []
        reuse_stats = {
            "total_requests": len(parsed_cmds),
            "eligible_read_requests": 0,
            "skipped_requests": 0,
            "artifact_hits": 0,
            "artifact_bound": 0,
            "artifact_misses": 0,
        }
        for cmd in parsed_cmds:
            if cmd.get("type") == "read" and cmd.get("reuse_enabled", True):
                reuse_stats["eligible_read_requests"] += 1
            else:
                reuse_stats["skipped_requests"] += 1

            artifact = find_reusable_artifact(cmd)
            if artifact_is_reusable(artifact):
                reuse_stats["artifact_hits"] += 1
                reuse_hits.append((cmd, artifact))
            else:
                if cmd.get("type") == "read" and cmd.get("reuse_enabled", True):
                    reuse_stats["artifact_misses"] += 1
                pending_cmds.append(cmd)
        reuse_stats["artifact_bound"] = process_reuse_hits(reuse_hits, worker_count)

    units = make_execution_units(pending_cmds)
    if reuse_stats is not None:
        reuse_stats["pending_requests"] = len(pending_cmds)
        reuse_stats["execution_units"] = len(units)
        reuse_stats["intra_batch_reuse_groups"] = sum(
            1 for unit in units if unit["type"] == "read" and len(unit["cmds"]) > 1
        )
        reuse_stats["intra_batch_reuse_candidates"] = sum(
            max(0, len(unit["cmds"]) - 1) for unit in units if unit["type"] == "read"
        )
        log(
            "reuse summary "
            + " ".join(f"{key}={value}" for key, value in reuse_stats.items())
        )
    units.sort(key=scheduling_score, reverse=True)
    log(f"batch scheduled units={len(units)} pending_requests={len(pending_cmds)}")

    selected_units = []
    for unit in units:
        if not resources_available(unit) or not within_tenant_budget(unit):
            log(f"unit deferred type={unit['type']} key={unit['key']}")
            continue
        log(f"unit selected type={unit['type']} key={unit['key']} score={scheduling_score(unit)}")
        selected_units.append(unit)

    if worker_count <= 1 or len(selected_units) <= 1:
        for unit in selected_units:
            execute_and_delete_unit(unit)
        return

    workers = min(worker_count, len(selected_units))
    log(f"batch executing units concurrently units={len(selected_units)} workers={workers}")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for unit in selected_units:
            log(f"batch submitting unit type={unit['type']} key={unit['key']} requests={len(unit['cmds'])}")
            futures.append(pool.submit(execute_and_delete_unit, unit))
        for future in as_completed(futures):
            future.result()

@diagnostics.execution("active_units", lambda cmd: [cmd])
def execute_single_cmd(cmd):
    if EXECUTOR_NOOP_ENABLED:
        return handle_noop(cmd)
    if cmd.get("type") == "read":
        return handle_read(cmd) is not None
    if cmd.get("type") == "write":
        return handle_write(cmd)
    log(f"unknown command type request_id={cmd.get('request_id')} type={cmd.get('type')}")
    set_state(cmd["request_id"], "FAILED")
    return False

def execute_and_delete_cmd(cmd):
    try:
        return execute_single_cmd(cmd)
    except Exception as exc:
        request_id = cmd.get("request_id")
        log(f"command failed request_id={request_id} type={cmd.get('type')} error={exc}")
        if request_id:
            set_state(request_id, "FAILED")
        return False
    finally:
        delete_cmd(cmd)

def execute_baseline_batch(cmds):
    log(f"baseline batch received messages={len(cmds)}")
    parsed_cmds = []
    for message in cmds:
        cmd = parse_cmd(message, message.get("_received_at_epoch_s"))
        if not cmd:
            continue
        parsed_cmds.append(cmd)

    if EXECUTOR_WORKER_COUNT <= 1 or len(parsed_cmds) <= 1:
        for cmd in parsed_cmds:
            log(f"baseline executing request_id={cmd.get('request_id')} type={cmd.get('type')}")
            execute_and_delete_cmd(cmd)
        return

    workers = min(EXECUTOR_WORKER_COUNT, len(parsed_cmds))
    log(f"baseline executing concurrently requests={len(parsed_cmds)} workers={workers}")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for cmd in parsed_cmds:
            log(f"baseline submitting request_id={cmd.get('request_id')} type={cmd.get('type')}")
            futures.append(pool.submit(execute_and_delete_cmd, cmd))
        for future in as_completed(futures):
            future.result()

def execute_baseline_message(message):
    cmd = parse_cmd(message, message.get("_received_at_epoch_s"))
    if not cmd:
        return False
    log(f"baseline executing request_id={cmd.get('request_id')} type={cmd.get('type')}")
    return execute_and_delete_cmd(cmd)

def select_batch_executor(mode):
    if mode == "baseline":
        return execute_baseline_batch
    if mode == "reuse":
        return execute_batch
    raise ValueError(f"unsupported executor scheduler mode: {mode}")

def receive_messages(queue_url, batch_size, wait_time):
    receive_id = diagnostics.begin_receive(batch_size, wait_time)
    receive_start = now()
    attributes = ["SentTimestamp"]
    if diagnostics.enabled:
        attributes += ["ApproximateReceiveCount", "ApproximateFirstReceiveTimestamp"]
    try:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=wait_time,
            AttributeNames=attributes,
        )
    except Exception as exc:
        response = getattr(exc, "response", {})
        diagnostics.end_receive(receive_id, now() - receive_start,
                                metadata=response.get("ResponseMetadata"),
                                error=response.get("Error", {}).get("Code", type(exc).__name__))
        raise
    receive_time_s = now() - receive_start
    received_at_epoch_s = time.time()
    messages = resp.get("Messages", [])
    for message in messages:
        message["_received_at_epoch_s"] = received_at_epoch_s
        if diagnostics.enabled:
            message["_diag_received"] = receive_start + receive_time_s
            message["_diag_receive_id"] = receive_id
    diagnostics.end_receive(receive_id, receive_time_s, len(messages), resp.get("ResponseMetadata"))
    return messages, receive_time_s

def adaptive_poll_legacy(
    queue_url,
    batch_executor,
    min_backoff=MIN_BACKOFF_SECONDS,
    max_backoff=MAX_BACKOFF_SECONDS,
    batch_size=POLL_BATCH_SIZE,
    wait_time=LONG_POLL_WAIT_SECONDS,
):
    backoff = min_backoff
    while True:
        cmds, receive_time_s = receive_messages(queue_url, batch_size, wait_time)
        if cmds:
            log(f"poll found messages={len(cmds)} sqs_receive_time={receive_time_s:.6f}s")
            backoff = min_backoff
            batch_executor(cmds)
        else:
            log(f"poll empty sqs_receive_time={receive_time_s:.6f}s sleep={backoff}")
            diagnostics.sleep(backoff, "backoff")
            backoff = min(2 * backoff, max_backoff)

def run_baseline_inflight_poll(
    queue_url,
    max_inflight=EXECUTOR_WORKER_COUNT,
    batch_size=POLL_BATCH_SIZE,
    scheduler_batch_size=SCHEDULER_BATCH_SIZE,
    scheduler_batch_wait=SCHEDULER_BATCH_WAIT_SECONDS,
    wait_time=LONG_POLL_WAIT_SECONDS,
    idle_sleep=POLL_IDLE_SLEEP_SECONDS,
    min_backoff=MIN_BACKOFF_SECONDS,
    max_backoff=MAX_BACKOFF_SECONDS,
):
    inflight = set()
    pending_cmds = []
    pending_started_at = None
    batch_size = min(batch_size, 10)
    scheduler_batch_size = max(1, scheduler_batch_size)
    backoff = min_backoff
    with ThreadPoolExecutor(max_workers=max_inflight) as pool:
        while True:
            received_any = False
            while len(inflight) < max_inflight and len(pending_cmds) < scheduler_batch_size:
                diagnostics.update(mode="baseline", inflight_slots=len(inflight),
                                   pending_messages=len(pending_cmds), backoff_s=backoff)
                batch_slots = scheduler_batch_size - len(pending_cmds)
                receive_count = min(batch_size, batch_slots)
                messages, receive_time_s = receive_messages(queue_url, receive_count, wait_time)
                if not messages:
                    # log(
                    #     "poll empty "
                    #     f"sqs_receive_time={receive_time_s:.6f}s "
                    #     f"inflight={len(inflight)} pending_messages={len(pending_cmds)} "
                    #     f"sleep={idle_sleep}"
                    # )
                    break

                received_any = True
                backoff = min_backoff
                if pending_started_at is None:
                    pending_started_at = now()
                parsed_count = 0
                for message in messages:
                    cmd = parse_cmd(message, message.get("_received_at_epoch_s"))
                    if cmd:
                        pending_cmds.append(cmd)
                        parsed_count += 1
                log(
                    "poll found "
                    f"messages={len(messages)} sqs_receive_time={receive_time_s:.6f}s "
                    f"inflight={len(inflight)} pending_messages={len(pending_cmds)} "
                    f"max_inflight={max_inflight}"
                )

            pending_age = now() - pending_started_at if pending_started_at is not None else 0
            should_submit = bool(pending_cmds) and (
                len(pending_cmds) >= scheduler_batch_size
                or pending_age >= scheduler_batch_wait
            )
            if should_submit:
                free_slots = max_inflight - len(inflight)
                if free_slots <= 0:
                    should_submit = False
            if should_submit:
                pending_cmds.sort(key=lambda cmd: cmd.get("priority", 0), reverse=True)
                selected_cmds = pending_cmds[:free_slots]
                pending_cmds = pending_cmds[free_slots:]
                pending_started_at = now() if pending_cmds else None
                log(
                    "baseline scheduler submit "
                    f"requests={len(selected_cmds)} pending_remaining={len(pending_cmds)} "
                    f"age={pending_age:.6f}s batch_size={scheduler_batch_size} "
                    f"batch_wait={scheduler_batch_wait:.6f}s "
                    f"priorities={[cmd.get('priority', 0) for cmd in selected_cmds]}"
                )
                diagnostics.submit(selected_cmds, pending_age, scheduler_batch_size, scheduler_batch_wait)
                for cmd in selected_cmds:
                    inflight.add(pool.submit(execute_and_delete_cmd, cmd))

            diagnostics.update(mode="baseline", inflight_slots=len(inflight),
                               pending_messages=len(pending_cmds), backoff_s=backoff)
            if inflight:
                timeout = idle_sleep
                if pending_cmds and pending_started_at is not None:
                    timeout = min(idle_sleep, max(0, scheduler_batch_wait - (now() - pending_started_at)))
                done, inflight = wait(inflight, timeout=timeout, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        future.result()
                    except Exception as exc:
                        log(f"baseline worker failed error={exc}")
            elif pending_cmds:
                remaining = max(0, scheduler_batch_wait - (now() - pending_started_at))
                sleep_time = min(idle_sleep, remaining)
                if sleep_time > 0:
                    log(
                        "baseline scheduler wait "
                        f"pending_messages={len(pending_cmds)} sleep={sleep_time:.6f}s"
                    )
                    diagnostics.sleep(sleep_time, "batch_wait")
            else:
                #log(f"poll idle sleep={backoff}")
                diagnostics.sleep(backoff, "backoff")
                if not received_any:
                    backoff = min(2 * backoff, max_backoff)

def run_reuse_inflight_poll(
    queue_url,
    max_inflight=EXECUTOR_WORKER_COUNT,
    batch_size=POLL_BATCH_SIZE,
    scheduler_batch_size=SCHEDULER_BATCH_SIZE,
    scheduler_batch_wait=SCHEDULER_BATCH_WAIT_SECONDS,
    wait_time=LONG_POLL_WAIT_SECONDS,
    idle_sleep=POLL_IDLE_SLEEP_SECONDS,
    min_backoff=MIN_BACKOFF_SECONDS,
    max_backoff=MAX_BACKOFF_SECONDS,
):
    inflight = {}
    pending_messages = []
    pending_started_at = None
    batch_size = min(batch_size, 10)
    scheduler_batch_size = max(1, scheduler_batch_size)
    max_batch_workers = max_inflight
    backoff = min_backoff
    with ThreadPoolExecutor(max_workers=max_batch_workers) as pool:
        while True:
            received_any = False
            inflight_slots = sum(inflight.values())
            while inflight_slots < max_inflight and len(pending_messages) < scheduler_batch_size:
                diagnostics.update(mode="reuse", inflight_slots=inflight_slots,
                                   inflight_batches=len(inflight), pending_messages=len(pending_messages),
                                   backoff_s=backoff)
                batch_slots = scheduler_batch_size - len(pending_messages)
                receive_count = min(batch_size, batch_slots)
                wait_time = 1 if not pending_messages and not inflight else 0
                messages, receive_time_s = receive_messages(queue_url, receive_count, wait_time)
                if not messages:
                    # log(
                    #     "poll empty "
                    #     f"sqs_receive_time={receive_time_s:.6f}s "
                    #     f"inflight_batches={len(inflight)} inflight_messages={inflight_slots} "
                    #     f"pending_messages={len(pending_messages)} sleep={idle_sleep}"
                    # )
                    break

                received_any = True
                backoff = min_backoff
                if pending_started_at is None:
                    pending_started_at = now()
                pending_messages.extend(messages)
                log(
                    "poll found "
                    f"messages={len(messages)} sqs_receive_time={receive_time_s:.6f}s "
                    f"inflight_batches={len(inflight)} inflight_messages={inflight_slots} "
                    f"pending_messages={len(pending_messages)} "
                    f"max_inflight={max_inflight}"
                )

            inflight_slots = sum(inflight.values())
            pending_age = now() - pending_started_at if pending_started_at is not None else 0
            should_submit = bool(pending_messages) and (
                len(pending_messages) >= scheduler_batch_size
                or pending_age >= scheduler_batch_wait
            )
            if should_submit:
                free_slots = max_inflight - inflight_slots
                if free_slots <= 0:
                    should_submit = False
            if should_submit:
                batch_messages = pending_messages[:free_slots]
                pending_messages = pending_messages[free_slots:]
                pending_started_at = now() if pending_messages else None
                batch_slots = len(batch_messages)
                log(
                    "scheduler batch submit "
                    f"messages={batch_slots} pending_remaining={len(pending_messages)} "
                    f"age={pending_age:.6f}s batch_size={scheduler_batch_size} "
                    f"batch_wait={scheduler_batch_wait:.6f}s"
                )
                diagnostics.submit(batch_messages, pending_age, scheduler_batch_size, scheduler_batch_wait)
                future = pool.submit(execute_batch, batch_messages, batch_slots)
                inflight[future] = batch_slots

            diagnostics.update(mode="reuse", inflight_slots=sum(inflight.values()),
                               inflight_batches=len(inflight), pending_messages=len(pending_messages),
                               backoff_s=backoff)
            if inflight:
                timeout = idle_sleep
                if pending_messages and pending_started_at is not None:
                    timeout = min(idle_sleep, max(0, scheduler_batch_wait - (now() - pending_started_at)))
                done, _ = wait(inflight.keys(), timeout=timeout, return_when=FIRST_COMPLETED)
                for future in done:
                    inflight.pop(future, None)
                    try:
                        future.result()
                    except Exception as exc:
                        log(f"reuse batch worker failed error={exc}")
            elif pending_messages:
                remaining = max(0, scheduler_batch_wait - (now() - pending_started_at))
                sleep_time = min(idle_sleep, remaining)
                if sleep_time > 0:
                    log(
                        "scheduler batch wait "
                        f"pending_messages={len(pending_messages)} sleep={sleep_time:.6f}s"
                    )
                    diagnostics.sleep(sleep_time, "batch_wait")
            else:
                #log(f"poll idle sleep={backoff}")
                diagnostics.sleep(backoff, "backoff")
                if not received_any:
                    backoff = min(2 * backoff, max_backoff)

def adaptive_poll(queue_url, scheduler_mode):
    if scheduler_mode == "baseline":
        run_baseline_inflight_poll(queue_url)
        return
    if scheduler_mode == "reuse":
        run_reuse_inflight_poll(queue_url)
        return
    raise ValueError(f"unsupported executor scheduler mode: {scheduler_mode}")

def main():
    log(
        "executor starting "
        f"domain_id={PRIVATE_DOMAIN_ID} "
        f"scheduler_mode={EXECUTOR_SCHEDULER_MODE} "
        f"worker_count={EXECUTOR_WORKER_COUNT} "
        f"scheduler_batch_size={SCHEDULER_BATCH_SIZE} "
        f"scheduler_batch_wait={SCHEDULER_BATCH_WAIT_SECONDS} "
        f"noop={EXECUTOR_NOOP_ENABLED}"
    )
    diagnostics.start(domain_id=PRIVATE_DOMAIN_ID, mode=EXECUTOR_SCHEDULER_MODE,
                      queue_backend=QUEUE_BACKEND, object_store_backend=OBJECT_STORE_BACKEND,
                      worker_count=EXECUTOR_WORKER_COUNT, min_backoff_s=MIN_BACKOFF_SECONDS,
                      max_backoff_s=MAX_BACKOFF_SECONDS, long_poll_wait_s=LONG_POLL_WAIT_SECONDS,
                      scheduler_batch_size=SCHEDULER_BATCH_SIZE,
                      scheduler_batch_wait_s=SCHEDULER_BATCH_WAIT_SECONDS)
    try:
        adaptive_poll(SQS_QUEUE_URL, EXECUTOR_SCHEDULER_MODE)
    finally:
        diagnostics.stop()

if __name__ == "__main__":
    main()
