import json
import time
import uuid
from datetime import datetime
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from config import (
    API_LOG_ENABLED,
    OBJECT_STORE_BACKEND,
    QUEUE_BACKEND,
    MINIO_ACCESS_KEY_ID,
    MINIO_SECRET_ACCESS_KEY,
    AWS_MAX_POOL_CONNECTIONS,
    AWS_REGION,
    EXECUTOR_RPC_TIMEOUT_SECONDS,
    EXECUTOR_RPC_URL,
    PRIVATE_DOMAINS_JSON,
    PRIVATE_STORAGE_API_TIMEOUT_SECONDS,
    PRIVATE_STORAGE_API_URL,
    REQUEST_EXECUTION_MODE,
    S3_BUCKET_RELEASED,
    S3_BUCKET_STAGED,
    S3_ENDPOINT_URL,
    SQS_ENDPOINT_URL,
    SQS_QUEUE_URL,
)
from db import engine

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

app = FastAPI()

DEFAULT_DOMAIN_ID = "default"

DEFAULT_DOMAIN_CONFIG = {
    "queue_url": SQS_QUEUE_URL,
    "rpc_url": EXECUTOR_RPC_URL,
    "storage_api_url": PRIVATE_STORAGE_API_URL,
    "staged_bucket": S3_BUCKET_STAGED,
    "released_bucket": S3_BUCKET_RELEASED,
}

def load_domain_configs():
    configs = {DEFAULT_DOMAIN_ID: dict(DEFAULT_DOMAIN_CONFIG)}
    if not PRIVATE_DOMAINS_JSON:
        return configs
    try:
        loaded = json.loads(PRIVATE_DOMAINS_JSON)
    except json.JSONDecodeError as exc:
        raise RuntimeError("PRIVATE_DOMAINS_JSON is invalid JSON") from exc
    for domain_id, domain_config in loaded.items():
        merged = dict(DEFAULT_DOMAIN_CONFIG)
        merged.update(domain_config or {})
        configs[str(domain_id)] = merged
    return configs

DOMAIN_CONFIGS = load_domain_configs()

def domain_id_or_default(domain_id: str | None):
    return domain_id or DEFAULT_DOMAIN_ID

def get_domain_config(domain_id: str | None):
    domain_id = domain_id_or_default(domain_id)
    config = DOMAIN_CONFIGS.get(domain_id)
    if not config:
        raise HTTPException(status_code=400, detail=f"unknown private storage domain: {domain_id}")
    return config

def now():
    return time.perf_counter()

def log(message):
    if API_LOG_ENABLED:
        print(f"[api] {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {message}", flush=True)

def log_timings(op, request_id, timings, total_start, **fields):
    parts = [f"{name}={value:.6f}s" for name, value in timings.items()]
    parts.append(f"total={now() - total_start:.6f}s")
    for name, value in fields.items():
        parts.append(f"{name}={value}")
    log(f"{op} timings request_id={request_id} " + " ".join(parts))

class RequestOptions(BaseModel):
    domain_id: str = DEFAULT_DOMAIN_ID
    priority: int = 0
    reuse_enabled: bool = True

class ReadReq(RequestOptions):
    requester_id: str
    target_spec: str

class UploadInitReq(BaseModel):
    domain_id: str = DEFAULT_DOMAIN_ID
    requester_id: str
    content_type: str = "application/octet-stream"
    size_bytes: int = 0
    filename: str = "object.bin"

class WriteReq(RequestOptions):
    requester_id: str
    upload_id: str
    target_spec: str

def validate_read_permission(req: ReadReq) -> bool:
    return True

def validate_write_permission(req: WriteReq) -> bool:
    return True

def process_read_data(data: bytes) -> bytes:
    return data

def process_write_data(data: bytes) -> bytes:
    return data

def update_request_state(request_id: str, state: str):
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE requests SET state=:state, updated_at=NOW() WHERE request_id=:id"),
            {"state": state, "id": request_id},
        )

def mark_request_enqueued_if_pending(request_id: str):
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE requests
                SET state='ENQUEUED', updated_at=NOW()
                WHERE request_id=:id AND state='SUBMITTED'
            """),
            {"id": request_id},
        )
        if result.rowcount:
            return "ENQUEUED"

        state = conn.execute(
            text("SELECT state FROM requests WHERE request_id=:id"),
            {"id": request_id},
        ).scalar_one_or_none()
        return state or "UNKNOWN"

def call_executor_rpc(path: str, payload: dict, domain_config: dict):
    rpc_url = domain_config.get("rpc_url")
    if not rpc_url:
        raise HTTPException(status_code=500, detail="EXECUTOR_RPC_URL is not configured")

    url = f"{rpc_url.rstrip('/')}{path}"
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=EXECUTOR_RPC_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8") or str(exc)
        raise HTTPException(status_code=502, detail=f"executor RPC failed: {detail}") from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"executor RPC unavailable: {exc}") from exc

def call_storage_api(path: str, domain_config: dict, method: str = "GET", data=None):
    storage_api_url = domain_config.get("storage_api_url")
    if not storage_api_url:
        raise HTTPException(status_code=500, detail="PRIVATE_STORAGE_API_URL is not configured")

    url = f"{storage_api_url.rstrip('/')}{path}"
    request = Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/octet-stream")
    try:
        with urlopen(request, timeout=PRIVATE_STORAGE_API_TIMEOUT_SECONDS) as response:
            return response.read()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8") or str(exc)
        raise HTTPException(status_code=502, detail=f"private storage API failed: {detail}") from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"private storage API unavailable: {exc}") from exc

def lookup_staged_s3_key(upload_id: str, domain_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT s3_key FROM staged_uploads WHERE upload_id=:id AND domain_id=:domain_id"),
            {"id": upload_id, "domain_id": domain_id},
        ).mappings().first()
    return row["s3_key"] if row else None

def release_request_with_artifact(request_id: str, artifact_id: str, s3_key: str, domain_id: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO artifacts(artifact_id, domain_id, request_id, s3_key, state)
                VALUES (:artifact_id, :domain_id, :request_id, :s3_key, 'READY')
            """),
            {"artifact_id": artifact_id, "domain_id": domain_id, "request_id": request_id, "s3_key": s3_key},
        )
        conn.execute(
            text("""
                UPDATE requests
                SET state='RELEASED', updated_at=NOW()
                WHERE request_id=:id AND domain_id=:domain_id
            """),
            {"id": request_id, "domain_id": domain_id},
        )
    return artifact_id

def execute_read_via_storage_api(request_id: str, req: ReadReq):
    total_start = now()
    timings = {}
    bytes_in = 0
    bytes_out = 0
    domain_id = domain_id_or_default(req.domain_id)
    domain_config = get_domain_config(domain_id)
    released_bucket = domain_config.get("released_bucket")
    if not released_bucket:
        raise HTTPException(status_code=500, detail=f"released bucket is not configured for domain: {domain_id}")
    log(f"storage_api read start request_id={request_id} domain_id={domain_id} target={req.target_spec}")
    t0 = now()
    update_request_state(request_id, "PROCESSING")
    timings["state_processing"] = now() - t0
    target = quote(req.target_spec, safe="")
    try:
        t0 = now()
        data = call_storage_api(f"/storage/exports/{target}", domain_config)
        timings["private_storage_get"] = now() - t0
        bytes_in = len(data)
    except HTTPException:
        timings["private_storage_get"] = now() - t0
        t0 = now()
        update_request_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "storage_api_read",
            request_id,
            timings,
            total_start,
            status="failed_private_storage_get",
            domain_id=domain_id,
            target=req.target_spec,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "FAILED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    t0 = now()
    processed_data = process_read_data(data)
    timings["process"] = now() - t0
    bytes_out = len(processed_data) if processed_data else 0
    if not processed_data:
        t0 = now()
        update_request_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "storage_api_read",
            request_id,
            timings,
            total_start,
            status="failed_empty_processed_data",
            domain_id=domain_id,
            target=req.target_spec,
            bytes_in=bytes_in,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "FAILED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    artifact_id = str(uuid.uuid4())
    s3_key = f"released/{request_id}/{artifact_id}.file"
    t0 = now()
    s3.put_object(Bucket=released_bucket, Key=s3_key, Body=processed_data)
    timings["s3_put_released"] = now() - t0
    t0 = now()
    release_request_with_artifact(request_id, artifact_id, s3_key, domain_id)
    timings["db_release_artifact"] = now() - t0
    log_timings(
        "storage_api_read",
        request_id,
        timings,
        total_start,
        status="released",
        domain_id=domain_id,
        target=req.target_spec,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
    )
    return {
        "request_id": request_id,
        "domain_id": domain_id,
        "state": "RELEASED",
        "artifact": {"artifact_id": artifact_id, "s3_key": s3_key},
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }

def execute_write_via_storage_api(request_id: str, req: WriteReq):
    total_start = now()
    timings = {}
    bytes_in = 0
    bytes_out = 0
    domain_id = domain_id_or_default(req.domain_id)
    domain_config = get_domain_config(domain_id)
    staged_bucket = domain_config.get("staged_bucket")
    if not staged_bucket:
        raise HTTPException(status_code=500, detail=f"staged bucket is not configured for domain: {domain_id}")
    log(f"storage_api write start request_id={request_id} domain_id={domain_id} upload_id={req.upload_id} target={req.target_spec}")
    t0 = now()
    update_request_state(request_id, "PROCESSING")
    timings["state_processing"] = now() - t0
    t0 = now()
    s3_key = lookup_staged_s3_key(req.upload_id, domain_id)
    timings["db_lookup_staged_upload"] = now() - t0
    if not s3_key:
        t0 = now()
        update_request_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "storage_api_write",
            request_id,
            timings,
            total_start,
            status="failed_missing_staged_upload",
            domain_id=domain_id,
            upload_id=req.upload_id,
            target=req.target_spec,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "FAILED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    try:
        t0 = now()
        obj = s3.get_object(Bucket=staged_bucket, Key=s3_key)
        timings["s3_get_staged"] = now() - t0
    except ClientError:
        timings["s3_get_staged"] = now() - t0
        t0 = now()
        update_request_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "storage_api_write",
            request_id,
            timings,
            total_start,
            status="failed_s3_get_staged",
            domain_id=domain_id,
            upload_id=req.upload_id,
            target=req.target_spec,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "FAILED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    t0 = now()
    raw_data = obj["Body"].read()
    timings["s3_read_staged_body"] = now() - t0
    bytes_in = len(raw_data)
    t0 = now()
    processed_data = process_write_data(raw_data)
    timings["process"] = now() - t0
    bytes_out = len(processed_data) if processed_data else 0
    if not processed_data:
        t0 = now()
        update_request_state(request_id, "REJECTED")
        timings["state_rejected"] = now() - t0
        log_timings(
            "storage_api_write",
            request_id,
            timings,
            total_start,
            status="rejected_empty_processed_data",
            domain_id=domain_id,
            upload_id=req.upload_id,
            target=req.target_spec,
            bytes_in=bytes_in,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "REJECTED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    target = quote(req.target_spec, safe="")
    upload_id = quote(req.upload_id, safe="")
    try:
        t0 = now()
        call_storage_api(f"/storage/exports/{target}/{upload_id}", domain_config, method="PUT", data=processed_data)
        timings["private_storage_put"] = now() - t0
    except HTTPException:
        timings["private_storage_put"] = now() - t0
        t0 = now()
        update_request_state(request_id, "FAILED")
        timings["state_failed"] = now() - t0
        log_timings(
            "storage_api_write",
            request_id,
            timings,
            total_start,
            status="failed_private_storage_put",
            domain_id=domain_id,
            upload_id=req.upload_id,
            target=req.target_spec,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
        )
        return {"request_id": request_id, "domain_id": domain_id, "state": "FAILED", "priority": req.priority, "reuse_enabled": req.reuse_enabled}

    t0 = now()
    update_request_state(request_id, "COMMITTED")
    timings["state_committed"] = now() - t0
    log_timings(
        "storage_api_write",
        request_id,
        timings,
        total_start,
        status="committed",
        domain_id=domain_id,
        upload_id=req.upload_id,
        target=req.target_spec,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
    )
    return {
        "request_id": request_id,
        "domain_id": domain_id,
        "state": "COMMITTED",
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }

def send_request_to_queue(payload: dict, domain_config: dict):
    payload = dict(payload)
    payload["_enqueued_at_epoch_s"] = time.time()
    queue_url = domain_config.get("queue_url")
    if not queue_url:
        raise HTTPException(status_code=500, detail=f"queue_url is not configured for domain: {payload.get('domain_id')}")
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
    )
    state = mark_request_enqueued_if_pending(payload["request_id"])
    return {
        "request_id": payload["request_id"],
        "domain_id": payload["domain_id"],
        "state": state,
        "priority": payload["priority"],
        "reuse_enabled": payload["reuse_enabled"],
    }

@app.get("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}

@app.post("/uploads/init")
def init_upload(req: UploadInitReq):
    domain_id = domain_id_or_default(req.domain_id)
    domain_config = get_domain_config(domain_id)
    staged_bucket = domain_config.get("staged_bucket")
    if not staged_bucket:
        raise HTTPException(status_code=500, detail=f"staged bucket is not configured for domain: {domain_id}")
    upload_id = str(uuid.uuid4())
    s3_key = f"staged/{upload_id}/{req.filename}"
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO staged_uploads(upload_id, domain_id, requester_id, s3_key, content_type, size_bytes, state)
                VALUES (:upload_id, :domain_id, :requester_id, :s3_key, :content_type, :size_bytes, 'INIT')
            """),
            {
                "upload_id": upload_id,
                "domain_id": domain_id,
                "requester_id": req.requester_id,
                "s3_key": s3_key,
                "content_type": req.content_type,
                "size_bytes": req.size_bytes,
            },
        )
    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": staged_bucket, "Key": s3_key, "ContentType": req.content_type},
        ExpiresIn=3600,
    )
    return {"upload_id": upload_id, "domain_id": domain_id, "bucket": staged_bucket, "key": s3_key, "upload_url": url}

@app.post("/uploads/{upload_id}/abort")
def abort_upload(upload_id: str):
    with engine.begin() as conn:
        has_request = conn.execute(
            text("SELECT 1 FROM requests WHERE upload_id=:id LIMIT 1"),
            {"id": upload_id},
        ).first()
        if has_request:
            return {"upload_id": upload_id, "state": "IN_USE"}

        result = conn.execute(
            text("""
                UPDATE staged_uploads
                SET state='ABORTED'
                WHERE upload_id=:id AND state='INIT'
            """),
            {"id": upload_id},
        )
        if result.rowcount:
            return {"upload_id": upload_id, "state": "ABORTED"}

        state = conn.execute(
            text("SELECT state FROM staged_uploads WHERE upload_id=:id"),
            {"id": upload_id},
        ).scalar_one_or_none()
    if state is None:
        raise HTTPException(status_code=404, detail="upload not found")
    return {"upload_id": upload_id, "state": state}

@app.post("/requests/read")
def submit_read(req: ReadReq):
    request_id = str(uuid.uuid4())
    domain_id = domain_id_or_default(req.domain_id)
    domain_config = get_domain_config(domain_id)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO requests(
                    request_id, domain_id, request_type, requester_id, target_spec,
                    priority, reuse_enabled, state
                )
                VALUES (
                    :request_id, :domain_id, 'read', :requester_id, :target_spec,
                    :priority, :reuse_enabled, 'SUBMITTED'
                )
            """),
            {
                "request_id": request_id,
                "domain_id": domain_id,
                "requester_id": req.requester_id,
                "target_spec": req.target_spec,
                "priority": req.priority,
                "reuse_enabled": req.reuse_enabled,
            },
        )

    if not validate_read_permission(req):
        update_request_state(request_id, "REJECTED")
        raise HTTPException(status_code=403, detail="read request is not authorized")

    payload = {
        "request_id": request_id,
        "domain_id": domain_id,
        "type": "read",
        "target_spec": req.target_spec,
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }
    if REQUEST_EXECUTION_MODE in {"executor_rpc", "rpc", "coupled"}:
        return call_executor_rpc("/execute/read", payload, domain_config)
    if REQUEST_EXECUTION_MODE == "storage_api":
        return execute_read_via_storage_api(request_id, req)
    if REQUEST_EXECUTION_MODE == "queue":
        return send_request_to_queue(payload, domain_config)
    raise HTTPException(status_code=500, detail=f"unsupported REQUEST_EXECUTION_MODE: {REQUEST_EXECUTION_MODE}")

@app.post("/requests/write")
def submit_write(req: WriteReq):
    request_id = str(uuid.uuid4())
    domain_id = domain_id_or_default(req.domain_id)
    domain_config = get_domain_config(domain_id)
    with engine.begin() as conn:
        staged_result = conn.execute(
            text("""
                UPDATE staged_uploads
                SET state='STAGED'
                WHERE upload_id=:id AND domain_id=:domain_id AND state='INIT'
            """),
            {"id": req.upload_id, "domain_id": domain_id},
        )
        if staged_result.rowcount == 0:
            row = conn.execute(
                text("SELECT domain_id, state FROM staged_uploads WHERE upload_id=:id"),
                {"id": req.upload_id},
            ).mappings().first()
            if row is None:
                raise HTTPException(status_code=404, detail="upload not found")
            if row["domain_id"] != domain_id:
                raise HTTPException(status_code=409, detail=f"upload belongs to domain {row['domain_id']}, not {domain_id}")
            raise HTTPException(status_code=409, detail=f"upload is not in INIT state: {row['state']}")

        conn.execute(
            text("""
                INSERT INTO requests(
                    request_id, domain_id, request_type, requester_id, target_spec, upload_id,
                    priority, reuse_enabled, state
                )
                VALUES (
                    :request_id, :domain_id, 'write', :requester_id, :target_spec, :upload_id,
                    :priority, :reuse_enabled, 'SUBMITTED'
                )
            """),
            {
                "request_id": request_id,
                "domain_id": domain_id,
                "requester_id": req.requester_id,
                "target_spec": req.target_spec,
                "upload_id": req.upload_id,
                "priority": req.priority,
                "reuse_enabled": req.reuse_enabled,
            },
        )

    if not validate_write_permission(req):
        update_request_state(request_id, "REJECTED")
        raise HTTPException(status_code=403, detail="write request is not authorized")

    payload = {
        "request_id": request_id,
        "domain_id": domain_id,
        "type": "write",
        "upload_id": req.upload_id,
        "target_spec": req.target_spec,
        "priority": req.priority,
        "reuse_enabled": req.reuse_enabled,
    }
    if REQUEST_EXECUTION_MODE in {"executor_rpc", "rpc", "coupled"}:
        return call_executor_rpc("/execute/write", payload, domain_config)
    if REQUEST_EXECUTION_MODE == "storage_api":
        return execute_write_via_storage_api(request_id, req)
    if REQUEST_EXECUTION_MODE == "queue":
        return send_request_to_queue(payload, domain_config)
    raise HTTPException(status_code=500, detail=f"unsupported REQUEST_EXECUTION_MODE: {REQUEST_EXECUTION_MODE}")

@app.get("/requests/{request_id}")
def get_request(request_id: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT request_id, domain_id, request_type, target_spec, priority, reuse_enabled, state
                FROM requests
                WHERE request_id=:id
            """),
            {"id": request_id},
        ).mappings().first()
    if not row:
        return {"error": "not found"}
    return dict(row)
