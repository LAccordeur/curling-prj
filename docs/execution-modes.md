# Execution Mode Configuration

This project supports four execution modes for comparing coupled and decoupled control/data-plane designs.

## Mode Summary

| Mode | API behavior | Private executor behavior | Reuse |
| --- | --- | --- | --- |
| coupled RPC baseline | API calls private executor RPC synchronously | RPC server executes each request immediately | No queue-level reuse |
| coupled storage API baseline | API synchronously transfers data through private storage API | Storage API only reads/writes private files | No queue-level reuse |
| decoupled baseline | API enqueues requests to SQS | Queue worker executes one request at a time | Disabled |
| decoupled reuse | API enqueues requests to SQS | Queue worker batch-schedules requests | Enabled |

Object storage is used in all modes. The main difference is whether the API waits for private-side execution synchronously, mediates the data transfer itself, or sends the request through the queue.

All modes support multiple private storage domains. When a request includes
`domain_id`, the API uses that domain's configured queue, RPC endpoint, storage
API endpoint, and buckets. If `domain_id` is omitted, the `default` domain is
used.

## Common Setup

Edit `inventory.env`, then regenerate environment files:

```bash
bash create_cluster_envs.sh
```

Start the control-plane API on node1:

```bash
bash deploy/control-plane/start.sh
```

Start the private executor service on node3:

```bash
bash deploy/private/start.sh
```

## Coupled RPC Baseline

Use this mode when the API should synchronously call the private executor. The queue is not used for request execution.

Set in `inventory.env`:

```bash
REQUEST_EXECUTION_MODE=executor_rpc
EXECUTOR_RPC_PORT=9008
EXECUTOR_RPC_TIMEOUT_SECONDS=300
```

`create_cluster_envs.sh` will generate:

```bash
EXECUTOR_RPC_URL=http://<NODE3_HOST>:9008
```

Flow:

```text
client -> API -> private executor RPC -> private storage/object storage -> API response
```

Expected request states:

```text
read:  SUBMITTED -> PROCESSING -> RELEASED
write: SUBMITTED -> PROCESSING -> COMMITTED
```

## Coupled Storage API Baseline

Use this mode when the API should synchronously access private storage through a storage API and mediate data transfer itself. The queue is not used, and data passes through the API service.

Set in `inventory.env`:

```bash
REQUEST_EXECUTION_MODE=storage_api
PRIVATE_STORAGE_API_PORT=9010
PRIVATE_STORAGE_API_TIMEOUT_SECONDS=300
```

`create_cluster_envs.sh` will generate:

```bash
PRIVATE_STORAGE_API_URL=http://<NODE3_HOST>:9010
```

Flow:

```text
read:  client -> API -> private storage API -> API -> released object storage -> API response
write: client -> staged object storage -> API -> private storage API -> private storage -> API response
```

Expected request states:

```text
read:  SUBMITTED -> PROCESSING -> RELEASED
write: SUBMITTED -> PROCESSING -> COMMITTED
```

## Decoupled Baseline

Use this mode when the API should enqueue requests, and the executor should process each request independently without reuse.

Set in `inventory.env`:

```bash
REQUEST_EXECUTION_MODE=queue
EXECUTOR_SCHEDULER_MODE=baseline
EXECUTOR_WORKER_COUNT=1
EXECUTOR_NOOP_ENABLED=false
EXECUTOR_SCHEDULER_BATCH_SIZE=40
EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS=0.02
```

Increase `EXECUTOR_WORKER_COUNT` to raise the single-process queue executor's
maximum in-flight work. The executor issues repeated SQS receives, each capped
at 10 messages, until the configured in-flight limit is filled. This is useful
when comparing against the coupled RPC server, whose synchronous FastAPI
handlers can run in a threadpool.

Baseline mode does not perform artifact reuse or read grouping, but it still
uses a short scheduler batch window to sort pending requests by `priority`
before submitting them as independent execution tasks.

Flow:

```text
client -> API -> SQS request queue -> private executor -> private storage/object storage
```

Expected request states:

```text
API side:  SUBMITTED -> ENQUEUED
executor:  ENQUEUED -> PROCESSING -> RELEASED or COMMITTED
```

For queue-overhead isolation experiments, enable no-op execution:

```bash
REQUEST_EXECUTION_MODE=queue
EXECUTOR_SCHEDULER_MODE=baseline
EXECUTOR_NOOP_ENABLED=true
```

In no-op execution, the API, queue polling, scheduling, queue deletion, and DB
state transitions still run. The executor skips private storage, object storage,
artifact creation, and data processing. Read requests finish as `RELEASED`, and
write requests finish as `COMMITTED`.

## Decoupled Reuse

Use this mode when the API should enqueue requests, and the executor should use batch scheduling and artifact reuse.

Set in `inventory.env`:

```bash
REQUEST_EXECUTION_MODE=queue
EXECUTOR_SCHEDULER_MODE=reuse
EXECUTOR_NOOP_ENABLED=false
EXECUTOR_SCHEDULER_BATCH_SIZE=40
EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS=0.02
```

Flow:

```text
client -> API -> SQS request queue -> private executor batch scheduler -> private storage/object storage
```

Reuse behavior:

- Existing READY artifacts may satisfy read requests directly.
- Remaining read requests are grouped by `target_spec`.
- Write requests are scheduled as single-request execution units.
- Execution units are sorted by scheduling score. The current score implementation primarily uses `priority`.
- The reuse scheduler uses a short micro-batch window before calling `execute_batch()`.
  This gives priority ordering and intra-batch reuse enough requests to compare while
  still allowing multiple batches to run concurrently for inter-batch artifact reuse.

## Private Service Selection

Node3 starts exactly one private service. `deploy/private/start.sh` selects it from `REQUEST_EXECUTION_MODE`:

```text
queue                  -> queue worker
executor_rpc/rpc/coupled -> executor RPC server
storage_api            -> private storage API
```

## Logging

Executor logs are enabled by default:

```bash
EXECUTOR_LOG_ENABLED=true
```

Disable them with:

```bash
EXECUTOR_LOG_ENABLED=false
```

## Executor Staging

By default, executor read/write processing keeps processed data in memory on the
hot path:

```bash
EXECUTOR_STAGING_ENABLED=false
```

Enable local staging for debugging intermediate files:

```bash
EXECUTOR_STAGING_ENABLED=true
```

When enabled, read results are written to `WORK_BASE/outbound` before upload,
and staged write objects are downloaded to `WORK_BASE/inbound` before
processing.
