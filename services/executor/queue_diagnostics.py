"""Opt-in queue diagnostics; never records bodies, receipt handles or credentials."""
import functools
import json
import threading
import time
from collections import Counter


class QueueDiagnostics:
    def __init__(self, enabled=False, interval=1.0):
        self.enabled = enabled
        self.interval = max(0.1, interval)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.state = {}
        self.counters = Counter(active_units=0, active_reuse_hit_tasks=0,
                                receive_calls=0, empty_receives=0, receive_errors=0,
                                received_messages=0, deleted_messages=0)
        self.receive_started = None
        self.sequence = 0
        self.batch_sequence = 0
        self.scheduler_updated = time.perf_counter()

    def emit(self, event, **fields):
        if self.enabled:
            print("[executor-diag] " + json.dumps(
                dict(event=event, timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     **fields), sort_keys=True), flush=True)

    def update(self, **fields):
        if self.enabled:
            with self.lock:
                self.state.update(fields)
                self.scheduler_updated = time.perf_counter()

    def add(self, **fields):
        if self.enabled:
            with self.lock:
                self.counters.update(fields)

    def snapshot(self):
        with self.lock:
            now = time.perf_counter()
            return dict(self.state, **self.counters,
                        scheduler_snapshot_age_s=now - self.scheduler_updated,
                        receive_in_progress=self.receive_started is not None,
                        receive_in_progress_s=now - self.receive_started if self.receive_started is not None else 0)

    def start(self, **fields):
        if not self.enabled or self.thread is not None:
            return
        self.emit("diagnostics_start", **fields)
        self.thread = threading.Thread(target=self._heartbeat, name="queue-diagnostics", daemon=True)
        self.thread.start()

    def _heartbeat(self):
        while not self.stop_event.wait(self.interval):
            self.emit("heartbeat", **self.snapshot())

    def stop(self):
        if self.thread is not None:
            self.stop_event.set()
            self.thread.join(timeout=2)

    def begin_receive(self, requested, wait_time):
        if not self.enabled:
            return None
        with self.lock:
            self.sequence += 1
            seq = self.sequence
            self.receive_started = time.perf_counter()
        self.emit("receive_start", receive_id=seq, requested=requested, wait_time_s=wait_time)
        return seq

    def end_receive(self, seq, duration, count=0, metadata=None, error=None):
        if not self.enabled:
            return
        with self.lock:
            self.receive_started = None
            self.counters.update(receive_calls=1, receive_time_s=duration,
                                 received_messages=count, receive_errors=int(error is not None),
                                 empty_receives=int(error is None and count == 0))
        metadata = metadata or {}
        self.emit("receive_end", receive_id=seq, elapsed_s=duration, returned=count,
                  retries=metadata.get("RetryAttempts"), http_status=metadata.get("HTTPStatusCode"),
                  aws_request_id=metadata.get("RequestId"), error=error)

    def submit(self, commands, pending_age, batch_size, batch_wait):
        if not self.enabled:
            return
        now = time.perf_counter()
        with self.lock:
            self.batch_sequence += 1
            batch_id = self.batch_sequence
        for cmd in commands:
            cmd["_diag_submitted"] = now
            cmd["_diag_batch_id"] = batch_id
            received = cmd.get("_diag_received")
            self.emit("request_dispatch", request_id=cmd.get("request_id"),
                      message_id=cmd.get("MessageId", cmd.get("_diag_message_id")),
                      batch_id=batch_id,
                      local_pending_s=now - received if received is not None else None)
        self.add(submitted_messages=len(commands), submitted_batches=1)
        self.emit("batch_submit", batch_id=batch_id, messages=len(commands),
                  pending_age_s=pending_age, deadline_overshoot_s=max(0, pending_age - batch_wait),
                  submit_reason="full" if len(commands) >= batch_size else "timeout_or_capacity")

    def sleep(self, duration, reason):
        if not self.enabled:
            time.sleep(duration)
            return
        start = time.perf_counter()
        self.update(sleep_reason=reason, sleep_requested_s=duration)
        try:
            time.sleep(duration)
        finally:
            elapsed = time.perf_counter() - start
            self.add(sleep_calls=1, sleep_time_s=elapsed, **{reason + "_sleep_time_s": elapsed})
            self.update(sleep_reason=None, sleep_requested_s=0)

    def execution(self, counter, commands):
        def decorate(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                if not self.enabled:
                    return func(*args, **kwargs)
                cmds = commands(*args, **kwargs)
                start = time.perf_counter()
                for cmd in cmds:
                    submitted = cmd.get("_diag_submitted")
                    self.emit("execution_start", request_id=cmd.get("request_id"),
                              batch_id=cmd.get("_diag_batch_id"),
                              dispatch_wait_s=start - submitted if submitted is not None else None)
                self.add(**{counter: 1})
                try:
                    return func(*args, **kwargs)
                finally:
                    self.add(**{counter: -1, "execution_tasks_finished": 1})
            return wrapped
        return decorate
