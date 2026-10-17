# Smoke Tests

## Execution Mode Test

`test_execution_mode.py` verifies one running execution mode end to end:

- API health check
- read request reaches `RELEASED`
- write upload/init/submit reaches `COMMITTED`

Run it after starting node1 and node3 in the mode you want to test.

```bash
bash deploy/scripts/test_execution_mode.sh coupled-rpc
bash deploy/scripts/test_execution_mode.sh coupled-storage-api
bash deploy/scripts/test_execution_mode.sh decoupled-baseline
bash deploy/scripts/test_execution_mode.sh decoupled-reuse
```

Each command assumes the deployment is already configured and restarted for that mode. The test does not switch service modes automatically.

Equivalent direct invocation:

```bash
CONTROL_API=http://<NODE1_HOST>:8000 \
python tests/smoke/test_execution_mode.py --mode decoupled-reuse
```

Optional overrides:

```bash
SMOKE_READ_TARGET=studyA
SMOKE_WRITE_TARGET=smoke_target
SMOKE_TIMEOUT_SECONDS=120
SMOKE_POLL_INTERVAL_SECONDS=2
```
