import os
from pathlib import Path

DB_URL = os.getenv("DB_URL")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
SQS_ENDPOINT_URL = os.getenv("SQS_ENDPOINT_URL")
OBJECT_STORE_BACKEND = os.getenv("OBJECT_STORE_BACKEND", "minio").lower()
QUEUE_BACKEND = os.getenv("QUEUE_BACKEND", "elasticmq").lower()
MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
AWS_MAX_POOL_CONNECTIONS = int(os.getenv("AWS_MAX_POOL_CONNECTIONS", "10"))
S3_BUCKET_STAGED = os.getenv("S3_BUCKET_STAGED")
S3_BUCKET_RELEASED = os.getenv("S3_BUCKET_RELEASED")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
PRIVATE_DOMAIN_ID = os.getenv("PRIVATE_DOMAIN_ID", "default")
PRIVATE_BASE = Path(os.getenv("PRIVATE_BASE", "./private-data"))
WORK_BASE = Path(os.getenv("WORK_BASE", "./work"))
EXECUTOR_LOG_ENABLED = os.getenv("EXECUTOR_LOG_ENABLED", "true").lower() not in {"0", "false", "no", "off"}
EXECUTOR_DIAGNOSTICS_ENABLED = os.getenv("EXECUTOR_DIAGNOSTICS_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS = float(os.getenv("EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS", "1"))
EXECUTOR_SCHEDULER_MODE = os.getenv("EXECUTOR_SCHEDULER_MODE", "reuse").lower()
EXECUTOR_STAGING_ENABLED = os.getenv("EXECUTOR_STAGING_ENABLED", "false").lower() not in {"0", "false", "no", "off"}
EXECUTOR_WORKER_COUNT = max(1, int(os.getenv("EXECUTOR_WORKER_COUNT", "1")))
EXECUTOR_NOOP_ENABLED = os.getenv("EXECUTOR_NOOP_ENABLED", "false").lower() not in {"0", "false", "no", "off"}
EXECUTOR_INFLIGHT_REUSE_ENABLED = os.getenv("EXECUTOR_INFLIGHT_REUSE_ENABLED", "true").lower() not in {"0", "false", "no", "off"}
EXECUTOR_MIN_BACKOFF_SECONDS = float(os.getenv("EXECUTOR_MIN_BACKOFF_SECONDS", "0.01"))
EXECUTOR_MAX_BACKOFF_SECONDS = float(os.getenv("EXECUTOR_MAX_BACKOFF_SECONDS", "1.0"))
EXECUTOR_SCHEDULER_BATCH_SIZE = max(1, int(os.getenv("EXECUTOR_SCHEDULER_BATCH_SIZE", str(EXECUTOR_WORKER_COUNT))))
EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS = float(os.getenv("EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS", "0.02"))
