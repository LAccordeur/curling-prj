# Queue Diagnostics

Enable in inventory.env, regenerate env files, sync code/config, and restart the
private executor:

```dotenv
EXECUTOR_DIAGNOSTICS_ENABLED=true
EXECUTOR_DIAGNOSTICS_INTERVAL_SECONDS=1
EXECUTOR_LOG_ENABLED=false
```

Diagnostics are independent of ordinary executor logs. Set EXECUTOR_LOG_ENABLED
to true when existing read/write/upload timings are also needed. Redirect stdout
and stderr to a file during diagnostic runs to reduce terminal rendering overhead.
Disable diagnostics for final performance measurements and measure their overhead.

Lines start with `[executor-diag]` followed by JSON. No message bodies, receipt
handles, credentials, or presigned URLs are included in these new records.

| Event | Meaning |
| --- | --- |
| diagnostics_start | Actual backend, worker, batching and backoff settings |
| receive_start / receive_end | Correlated receive ID, requested/returned count, elapsed time, SDK retries, HTTP status and AWS request ID; includes empty replies and failures |
| message_received | Request/message IDs, receive count, original sent/first-receive timestamps and local receive wall time |
| request_dispatch | Receive-to-submit local pending time and batch ID; reuse raw messages are correlated through message_id |
| batch_submit | Pending age, deadline overshoot and submission reason (full or timeout/capacity-limited) |
| execution_start | Submit-to-unit-start delay, including reuse batch preprocessing and worker waiting, not just thread-pool waiting |
| delete_end / delete_error | Acknowledgement time and SDK metadata |
| heartbeat | Scheduler snapshot, snapshot age, inflight slots, pending messages, active execution units, active artifact-hit tasks, receive in-progress duration and cumulative counters |

Heartbeat runs on a separate thread, so it can report a blocked receive. Scheduler
snapshots may be stale while the polling thread is blocked: inspect
`scheduler_snapshot_age_s`. `active_units` includes processing/lock waiting but
excludes the following message deletion. Artifact-hit tasks include binding and
deletion. `execution_tasks_finished` counts returned or raised execution tasks,
not successfully completed requests. Sleep and receive counters are cumulative;
subtract consecutive samples for interval rates. Wall timestamps across machines
require clock synchronization; local duration measurements use a monotonic clock.

Interpretation:

- A growing receive_in_progress_s suggests a call still blocked or retrying.
- Increasing empty_receives with no active units suggests the poller is running
  but obtaining no messages; it does not prove that the queue is globally empty.
- receive_count greater than one means a message was received previously. Check
  for other consumers, previous failed calls, and visibility timeout settings.
- Large local_pending_s/deadline_overshoot_s identifies local batching delay.
- Occupied inflight slots with few active units can include batch preprocessing,
  deletion or whole-batch completion waits; it is not automatically worker load.
- No heartbeat does not by itself prove the process died: output blocking,
  process suspension or CPU starvation can also prevent logging.

This instrumentation does not change scheduling, retry or visibility behavior and
does not issue additional queue-attribute API calls. Independently inspect SQS
VisibilityTimeout, DelaySeconds and visible/not-visible/delayed message counts
during a stall. Use a dedicated queue; do not use receive-message or console
message polling as a monitoring tool during benchmarks.
