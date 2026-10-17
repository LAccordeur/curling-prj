# Curling Prototype

Curling is a multi-node prototype for studying coupled and decoupled control/data-plane execution in a cross-boundary data exchange system.

The control plane accepts and validates read/write requests. The private side executes requests against private storage and exchanges data through object storage.

## Deployment Roles

- **node1: control plane**
  - PostgreSQL
  - Request API
- **node2: legacy exchange layer**
  - retained in `inventory.env` for compatibility
- **node5: object storage**
  - MinIO object storage
- **node6: request queue**
  - ElasticMQ request queue
- **node3: private domain**
  - private storage
  - queue worker, executor RPC server, or private storage API
- **additional private domains**
  - optional extra private storage nodes, each with its own queue, buckets, and executor config
- **node4: external client**
  - request drivers and smoke tests

## Execution Modes

Select one mode with `REQUEST_EXECUTION_MODE` in `inventory.env`.

| Mode | Key config | Private service |
| --- | --- | --- |
| Coupled RPC baseline | `REQUEST_EXECUTION_MODE=executor_rpc` | executor RPC server |
| Coupled storage API baseline | `REQUEST_EXECUTION_MODE=storage_api` | private storage API |
| Decoupled baseline | `REQUEST_EXECUTION_MODE=queue`, `EXECUTOR_SCHEDULER_MODE=baseline` | queue worker |
| Decoupled reuse | `REQUEST_EXECUTION_MODE=queue`, `EXECUTOR_SCHEDULER_MODE=reuse` | queue worker with batch scheduling/reuse |

Set `EXECUTOR_NOOP_ENABLED=true` with queue mode to isolate API, queue, executor polling, scheduling, and DB state-transition overhead without private/S3 data I/O.

`deploy/private/start.sh` starts exactly one private service based on `REQUEST_EXECUTION_MODE`.

See [docs/execution-modes.md](docs/execution-modes.md) for mode details, state flows, and configuration examples.

## Quick Start

From the admin machine:

```bash
bash create_inventory.sh
vim inventory.env
bash create_cluster_envs.sh
bash sync_to_nodes.sh
bash bootstrap_cluster.sh --init-sample-data
```

Then start long-running services:

```bash
# node1
bash deploy/control-plane/start.sh

# node3
bash deploy/private/start.sh
```

Use `tmux`, `screen`, or `systemd` for long-running deployments.

## Multiple Private Domains

The default deployment uses one private domain named `default`. To route
requests across multiple private storage domains, define `PRIVATE_DOMAINS` in
`inventory.env` and optionally override each domain's host, queue, bucket, and
storage paths:

```bash
PRIVATE_DOMAINS="domain-a domain-b"

DOMAIN_DOMAIN_A_PRIVATE_HOST=10.0.0.13
DOMAIN_DOMAIN_B_PRIVATE_HOST=10.0.0.17
DOMAIN_DOMAIN_A_QUEUE_NAME=request-queue-domain-a
DOMAIN_DOMAIN_B_QUEUE_NAME=request-queue-domain-b
DOMAIN_DOMAIN_A_STAGED_BUCKET=exchange-staged-domain-a
DOMAIN_DOMAIN_A_RELEASED_BUCKET=exchange-released-domain-a
DOMAIN_DOMAIN_B_STAGED_BUCKET=exchange-staged-domain-b
DOMAIN_DOMAIN_B_RELEASED_BUCKET=exchange-released-domain-b
```

Then regenerate env files and initialize exchange resources:

```bash
bash create_cluster_envs.sh
bash deploy/exchange/init_exchange.sh
```

The generated API env contains `PRIVATE_DOMAINS_JSON`, which maps each
`domain_id` to its queue, RPC endpoint, storage API endpoint, and buckets. Each
private domain gets its own env file:

```text
deploy/private/domain-a.env
deploy/private/domain-b.env
```

Start a private service for a specific domain with:

```bash
bash deploy/private/start.sh domain-a
bash deploy/private/start.sh domain-b
```

Requests can include `domain_id`; if omitted, the API uses `default`:

```json
{
  "domain_id": "domain-a",
  "requester_id": "alice",
  "target_spec": "read_small_00001"
}
```

See [docs/multi-node-deployment.md](docs/multi-node-deployment.md) for a more
detailed multi-domain setup.

## Running Requests

From node4 or any host with access to the API:

```bash
bash deploy/client/run_read.sh
bash deploy/client/run_write.sh
```

The client API endpoint is generated into `deploy/client/.env` as `CONTROL_API`.

## Smoke Tests

Run the mode-specific smoke test after configuring and restarting services for that mode:

```bash
bash deploy/scripts/test_execution_mode.sh coupled-rpc
bash deploy/scripts/test_execution_mode.sh coupled-storage-api
bash deploy/scripts/test_execution_mode.sh decoupled-baseline
bash deploy/scripts/test_execution_mode.sh decoupled-reuse
```

The test submits one read and one write request and checks for final states.

## Important Files

- `inventory.env.example`: template for cluster hosts, ports, and execution mode.
- `create_cluster_envs.sh`: generates per-role `.env` files.
- `deploy/control-plane/init_db.sql`: PostgreSQL schema.
- `services/api/main.py`: request API and coupled-mode control flow.
- `services/executor/executor.py`: queue worker, baseline scheduler, and reuse scheduler.
- `services/executor/rpc_server.py`: coupled RPC baseline server.
- `services/executor/storage_server.py`: coupled storage API baseline server.
- `docs/multi-node-deployment.md`: detailed deployment workflow.
- `docs/execution-modes.md`: execution mode configuration.

## Notes

- Real `.env` files are not committed.
- The generated scripts assume Ubuntu/Debian-like machines with `sudo`.
- Object storage is used in all execution modes.
- Coupled modes are synchronous from the API path. Decoupled modes return after enqueue and complete asynchronously.
