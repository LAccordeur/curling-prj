# Benchmark Driver

`run_benchmark.py` drives end-to-end read/write requests against the currently running Curling deployment.

It measures:

- admission latency: submit request call duration
- completion latency: submit request to final state
- operation latency: full client operation, including staged upload for writes
- throughput: completed successful requests per second
- final state distribution

Results are written to:

```text
experiments/benchmark/results/*.csv
experiments/benchmark/results/*.summary.json
```

## Plot CDF By Domain

Generate a completion-latency CDF from a benchmark CSV, with one line per
`domain_id`:

```bash
python experiments/benchmark/plot_cdf_by_domain.py \
  experiments/benchmark/results/<result>.csv
```

Write to a specific image path:

```bash
python experiments/benchmark/plot_cdf_by_domain.py \
  experiments/benchmark/results/<result>.csv \
  --output experiments/benchmark/results/completion-cdf.pdf
```

The default metric is `completion_latency_s`. You can also plot another numeric
column:

```bash
python experiments/benchmark/plot_cdf_by_domain.py \
  experiments/benchmark/results/<result>.csv \
  --metric admission_latency_s \
  --xscale log
```

Group the CDF by object/data size instead of domain:

```bash
python experiments/benchmark/plot_cdf_by_domain.py \
  experiments/benchmark/results/<result>.csv \
  --group-by object_size_label
```

You can also group by the raw byte size:

```bash
python experiments/benchmark/plot_cdf_by_domain.py \
  experiments/benchmark/results/<result>.csv \
  --group-by object_size_bytes
```

This plotting helper requires `matplotlib`.

## Run One Mode

Configure `inventory.env`, regenerate env files, restart node1 and node3, then run:

```bash
bash deploy/scripts/reset_benchmark_state.sh

bash deploy/scripts/run_benchmark.sh coupled-rpc \
  --workload mixed \
  --total-requests 100 \
  --concurrency 10
```

The first positional argument is a label used in output files. It does not switch the running services.

`reset_benchmark_state.sh` clears control-plane metadata, staged/released object storage, and the request queue. It does not remove private storage datasets.

## Generated Dataset and Trace

For reproducible experiments, generate a fixed dataset and request trace first. The generator is role-based:

- `admin`: generate only the trace JSON and metadata
- `private`: materialize read files under the private storage export directory
- `client`: materialize write input files under the client benchmark directory

On the admin machine:

```bash
python experiments/benchmark/generate_dataset.py \
  --role admin \
  --workload-json experiments/workloads/read_heavy_reuse_medium.json \
  --output-dir experiments/benchmark/generated/read-heavy-reuse \
  --seed 1
```

The repository includes these workload configs:

```text
experiments/workloads/read_heavy_reuse.json
experiments/workloads/mixed.json
experiments/workloads/write_heavy_no_reuse.json
```

You can override any workload field from the command line, for example:

```bash
python experiments/benchmark/generate_dataset.py \
  --role admin \
  --workload-json experiments/workloads/read_heavy_reuse_medium.json \
  --output-dir experiments/benchmark/generated/read-heavy-reuse \
  --total-requests 2000 \
  --read-target-count 20 \
  --priority-policy small-first
```

Sync the repository or generated trace to all nodes. Then on node3:

```bash
python experiments/benchmark/generate_dataset.py \
  --role private \
  --trace-json experiments/benchmark/generated/read-heavy-reuse/read-heavy-reuse.trace.json \
  --private-export-dir private-data/exports
```

On node4:

```bash
python experiments/benchmark/generate_dataset.py \
  --role client \
  --trace-json experiments/benchmark/generated/read-heavy-reuse/read-heavy-reuse.trace.json \
  --write-input-dir experiments/benchmark/generated/read-heavy-reuse/writes
```

Then run the same trace against each mode:

```bash
bash deploy/scripts/run_benchmark.sh decoupled-reuse \
  --trace-json experiments/benchmark/generated/read-heavy-reuse/read-heavy-reuse.trace.json \
  --trace-base-dir experiments/benchmark/generated/read-heavy-reuse \
  --concurrency 10
```

When `--trace-json` is provided, the built-in workload generator is bypassed. Read requests use the generated target names, and write requests upload the generated input files. Write files are used once in the trace; read targets may repeat. Relative write paths in the trace are resolved from `--trace-base-dir`, or from the trace file directory if not provided.

Benchmark requests use `reuse_enabled=true`. Request priority comes from the trace; randomized workloads use priority `0`. Trace requests may include `domain_id`; if omitted, `run_benchmark.py` uses `--domain-id` or `default`.

To generate traces across multiple private domains, pass a domain mix:

```bash
python experiments/benchmark/generate_dataset.py \
  --role admin \
  --workload-json experiments/workloads/mixed.json \
  --output-dir experiments/benchmark/generated/mixed-domains \
  --domain-mix domain-a:0.7,domain-b:0.3
```

If `--domain-mix` is omitted, every generated request uses `--domain-id`
which defaults to `default`.

## Four Mode Examples

Run each command after configuring and restarting the deployment for that mode:

```bash
bash deploy/scripts/run_benchmark.sh coupled-rpc --workload mixed --total-requests 100 --concurrency 10
bash deploy/scripts/run_benchmark.sh coupled-storage-api --workload mixed --total-requests 100 --concurrency 10
bash deploy/scripts/run_benchmark.sh decoupled-baseline --workload mixed --total-requests 100 --concurrency 10
bash deploy/scripts/run_benchmark.sh decoupled-reuse --workload mixed --total-requests 100 --concurrency 10
```

## Built-in Workloads

```text
read-heavy   90% read, 10% write
mixed        50% read, 50% write
write-heavy  10% read, 90% write
```

## Object Size Mix

Each request is assigned an object size label from a configurable distribution.

Default size profile:

```text
small   = 1024 bytes
medium  = 1048576 bytes
large   = 10485760 bytes
```

Default size mix:

```text
small:0.7,medium:0.2,large:0.1
```

Override sizes:

```bash
--size-profile small=4096,medium=1048576,large=104857600
```

Override ratios:

```bash
--size-mix small:0.5,medium:0.3,large:0.2
```

Environment variable equivalents:

```bash
BENCH_SIZE_PROFILE=small=1024,medium=1048576,large=10485760
BENCH_SIZE_MIX=small:0.7,medium:0.2,large:0.1
```

## Priority Policy

Generated traces support two priority policies:

```bash
--priority-policy uniform --uniform-priority 0
```

Every request gets the same priority.

```bash
--priority-policy small-first --priority-by-size small:2,medium:1,large:0
```

Small-object requests get higher priority by default. The priority is written into the trace and submitted by `run_benchmark.py`.

Read requests use `studyA` by default. Override with:

```bash
--read-targets studyA,studyB
```

Only use targets that exist in private storage for the current deployment.

## Suggested Matrix

For a compact experiment:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label>
```

By default this runs `read-heavy-reuse` at concurrency `1`, `10`, `20`, and
`40`. Request count is read from each workload JSON file. The script generates
fixed traces from workload JSON files and runs `run_benchmark.py` with
`--trace-json` and `--trace-base-dir`. It resets benchmark metadata, staged and
released objects, and the request queue before each point.

For each workload, the script prepares data in this order:

```text
generate trace
generate read files under private-data/exports on the client/admin machine
run sync_to_nodes.sh to copy read files to the private node
generate client write files under the trace directory
run benchmark for each concurrency level
```

The default generated workloads are:

```text
read-heavy-reuse
```

The default local read-file directory is `private-data/exports`. Override it
when needed:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label> \
  --private-export-dir private-data/exports
```

Skip automatic syncing only if the private node already has the matching read
files:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label> --no-sync-read-data
```

Override the compact matrix:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label> \
  --workloads read_heavy_reuse,mixed,write_heavy_no_reuse \
  --concurrency 1,10,20,50
```

Override request count for every workload:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label> \
  --total-requests 500
```

`--workloads` is resolved from filenames under `experiments/workloads`. You can
pass `read_heavy_reuse`, `read-heavy-reuse`, `read_heavy_reuse.json`, or an
explicit JSON path.

Pass additional `run_benchmark.py` arguments after `--`:

```bash
bash deploy/scripts/run_compact_benchmark.sh <mode-label> -- --timeout 600
```

Repeat the script for each execution mode after restarting services in that mode.

## Custom Workload JSON

You can pass a workload JSON to `generate_dataset.py`:

```bash
python experiments/benchmark/generate_dataset.py \
  --role admin \
  --workload-json experiments/workloads/sample_workload.json \
  --output-dir experiments/benchmark/generated/sample
```

Supported fields:

```json
{
  "name": "sample-mixed-workload",
  "total_requests": 1000,
  "read_ratio": 0.5,
  "read_target_count": 100,
  "read_repeat_mode": "zipf",
  "zipf_alpha": 0.8,
  "size_profile": {
    "small": 1024,
    "medium": 1048576,
    "large": 10485760
  },
  "size_mix": {
    "small": 0.7,
    "medium": 0.2,
    "large": 0.1
  },
  "priority_policy": "uniform",
  "uniform_priority": 0,
  "domain_id": "default",
  "domain_mix": ""
}
```

For formal comparisons, prefer `generate_dataset.py` plus `--trace-json` so every mode receives the same request sequence.

`read_repeat_mode` can be `zipf`, `uniform`, or `unique`. Use `unique` for no-reuse read baselines; it avoids repeating a target until the generated target pool is exhausted.

## Priority Scheduling Experiment

`run_priority_benchmark.py` creates two matched traces from `generate_dataset.py`:
one with uniform priority and one with size-aware priority where small objects
receive higher priority. It then runs both traces and writes a comparison report
grouped by size and priority.

Recommended executor settings:

```bash
REQUEST_EXECUTION_MODE=queue
EXECUTOR_SCHEDULER_MODE=baseline
EXECUTOR_WORKER_COUNT=4
EXECUTOR_SCHEDULER_BATCH_SIZE=40
EXECUTOR_SCHEDULER_BATCH_WAIT_SECONDS=0.1
```

Run:

```bash
deploy/scripts/run_priority_benchmark.sh \
  --total-requests 120 \
  --concurrency 40 \
  --size-mix small:0.5,medium:0.3,large:0.2 \
  --priority-by-size small:2,medium:1,large:0
```

If the client and private executor run on different machines, pass a sync command
that copies generated read files to the private node:

```bash
deploy/scripts/run_priority_benchmark.sh \
  --sync-command "bash sync_to_nodes.sh"
```

The report includes `p95_improvement_ratio`; positive values mean the
size-aware priority run reduced completion p95 relative to uniform priority.

## Object Store Microbenchmark

Use `experiments/object_store_benchmark.py` to measure object-store PUT/GET
latency and throughput without API, DB, queue, or executor work.

The script reads object-store defaults from `deploy/exchange/.env`:

```bash
python experiments/object_store_benchmark.py \
  --op put \
  --object-size 8MB \
  --requests 300 \
  --concurrency 40 \
  --output-json experiments/results/object-put-8m-c40.json
```

Download benchmark:

```bash
python experiments/object_store_benchmark.py \
  --op get \
  --object-size 8MB \
  --requests 300 \
  --concurrency 40
```

Mixed PUT/GET benchmark:

```bash
python experiments/object_store_benchmark.py \
  --op mixed \
  --object-size 8MB \
  --requests 300 \
  --concurrency 40
```

Useful overrides:

```bash
--endpoint-url http://<minio-host>:9000
--bucket exchange-staged-domain-a
--max-pool-connections 80
--records-csv experiments/results/object-put-8m-c40.csv
--no-cleanup
```

Run the same matrix from the home client, campus client node, and private
executor node to separate VPN/client-path bottlenecks from campus object-store
bottlenecks.
