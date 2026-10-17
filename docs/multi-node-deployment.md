# Multi-Node Deployment Guide

## Assumptions

- node1 runs PostgreSQL and the Request API.
- node2 is retained as the legacy exchange node in `inventory.env`.
- node3 runs the executor and stores private data.
- optional extra private nodes can run additional private storage domains.
- node4 runs the request client.
- node5 runs MinIO object storage.
- node6 runs ElasticMQ.
- the same repository path is used on all nodes, e.g. `/home/USER/curling`.

## Admin-machine workflow

```bash
bash create_inventory.sh
vim inventory.env
bash create_cluster_envs.sh
bash sync_to_nodes.sh
bash bootstrap_cluster.sh --init-sample-data
```

## Per-node workflow

If you prefer to bootstrap manually on each machine:

```bash
bash bootstrap_node.sh control
bash init_db.sh
```

```bash
bash bootstrap_node.sh object-storage
```

```bash
bash bootstrap_node.sh queue
```

On either exchange node after both services are reachable:

```bash
bash deploy/exchange/init_exchange.sh
```

```bash
bash bootstrap_node.sh private
bash deploy/scripts/init_sample_private_data.sh
```

```bash
bash bootstrap_node.sh client
```

## Start long-running services

### node1
```bash
bash deploy/control-plane/start.sh
```

### node3
```bash
bash deploy/private/start.sh
```

Use `tmux`, `screen`, or `systemd` in a real deployment.

## Multiple private storage domains

By default the deployment has one private domain named `default`. A multi-domain
deployment keeps the same control-plane API, object storage service, and queue
service, but creates separate logical resources for each private domain:

```text
domain_id -> queue_url
domain_id -> staged bucket
domain_id -> released bucket
domain_id -> private executor/storage service URL
```

Each private executor still serves exactly one domain. The API chooses the
target domain from the request's `domain_id` field.

### Configure domains

Add domains to `inventory.env`:

```bash
PRIVATE_DOMAINS="domain-a domain-b"

DOMAIN_DOMAIN_A_PRIVATE_HOST=10.0.0.13
DOMAIN_DOMAIN_B_PRIVATE_HOST=10.0.0.17
```

Domain-specific variable names are derived by uppercasing the domain id and
replacing non-alphanumeric characters with underscores. For example,
`domain-a` becomes `DOMAIN_DOMAIN_A_*`.

Common optional overrides:

```bash
DOMAIN_DOMAIN_A_QUEUE_HOST=10.0.0.16
DOMAIN_DOMAIN_A_OBJECT_HOST=10.0.0.15
DOMAIN_DOMAIN_A_QUEUE_NAME=request-queue-domain-a
DOMAIN_DOMAIN_A_STAGED_BUCKET=exchange-staged-domain-a
DOMAIN_DOMAIN_A_RELEASED_BUCKET=exchange-released-domain-a
DOMAIN_DOMAIN_A_PRIVATE_BASE=${DEPLOY_BASE}/private-data-domain-a
DOMAIN_DOMAIN_A_WORK_BASE=${DEPLOY_BASE}/work-domain-a
DOMAIN_DOMAIN_A_EXECUTOR_RPC_PORT=9008
DOMAIN_DOMAIN_A_PRIVATE_STORAGE_API_PORT=9010
```

If an override is omitted, `create_cluster_envs.sh` derives a value from the
legacy single-domain settings. For example, `domain-a` defaults to:

```text
queue:           request-queue-domain-a
staged bucket:   exchange-staged-domain-a
released bucket: exchange-released-domain-a
```

### Generate environment files

Run:

```bash
bash create_cluster_envs.sh
```

This generates:

```text
deploy/control-plane/.env
deploy/exchange/.env
deploy/private/.env
deploy/private/domain-a.env
deploy/private/domain-b.env
deploy/client/.env
```

The control-plane env includes `PRIVATE_DOMAINS_JSON`, which the API uses for
all execution modes:

```text
queue mode       domain_id -> queue_url
executor_rpc     domain_id -> rpc_url
storage_api      domain_id -> storage_api_url and buckets
```

### Initialize queues and buckets

After MinIO and ElasticMQ are reachable, run:

```bash
bash deploy/exchange/init_exchange.sh
```

The script creates every configured domain's staged bucket, released bucket,
and request queue.

### Start private services

On each private node, start the service for that domain:

```bash
bash deploy/private/start.sh domain-a
```

or explicitly provide an env file:

```bash
PRIVATE_ENV_FILE=deploy/private/domain-a.env bash deploy/private/start.sh
```

The selected env controls:

```text
PRIVATE_DOMAIN_ID
SQS_QUEUE_URL
S3_BUCKET_STAGED
S3_BUCKET_RELEASED
PRIVATE_BASE
WORK_BASE
```

### Submit domain-specific requests

Read and write requests may include `domain_id`:

```json
{
  "domain_id": "domain-a",
  "requester_id": "alice",
  "target_spec": "read_medium_00001",
  "priority": 1
}
```

Upload initialization should also use the same domain:

```json
{
  "domain_id": "domain-a",
  "requester_id": "alice",
  "filename": "object.bin"
}
```

The write request must use the same `domain_id` as the upload. The API rejects
a write if the `upload_id` belongs to a different domain.
# Mixed MinIO and AWS SQS Credentials

For MinIO + AWS SQS, set these values in inventory.env:

```dotenv
OBJECT_STORE_BACKEND=minio
QUEUE_BACKEND=aws_sqs
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=<your-account-id>
AWS_PROFILE=curling
MINIO_ROOT_USER=<minio-user>
MINIO_ROOT_PASSWORD=<minio-password>
```

Optional MINIO_ACCESS_KEY_ID and MINIO_SECRET_ACCESS_KEY override the root
credentials for application access. Keep SQS_ENDPOINT_URL unset/empty for AWS.
The generators write MinIO credentials under MINIO_* only. AWS S3/SQS clients
use the SDK credential provider chain, including profiles and temporary tokens.
ElasticMQ clients use independent dummy credentials.

When upgrading, remove any MinIO values previously stored in AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, or AWS_SESSION_TOKEN in inventory.env and launch environments.
Do not remove valid AWS credentials required by your chosen authentication method.
Regenerate env files with `bash create_cluster_envs.sh`, sync code/config, and
restart the API and private executors. Existing generated env files are not
automatically migrated. Standalone create_control_env.sh/create_private_env.sh
use the same backend and credential rules.

Install the named profile separately on every node making AWS calls, including
the API, executor, and the machine running init/reset or microbenchmarks. Verify
with `aws sts get-caller-identity --profile curling`. Credentials are not copied
from ~/.aws by the env generators. Do not publish credentials in logs or git.
