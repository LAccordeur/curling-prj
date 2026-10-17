"""Offline tests; no DB, queue or object-store requests."""
import ast
import importlib.util
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("queue_diagnostics", ROOT / "services/executor/queue_diagnostics.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
QueueDiagnostics = module.QueueDiagnostics


def receive_factory(diag, sqs):
    tree = ast.parse((ROOT / "services/executor/executor.py").read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "receive_messages")
    scope = dict(diagnostics=diag, sqs=sqs, now=time.perf_counter, time=time)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<receive_messages>", "exec"), scope)
    return scope["receive_messages"]


class DiagnosticTests(unittest.TestCase):
    def test_empty_receive_and_retry_metadata(self):
        diag = QueueDiagnostics(True)
        diag.emit = Mock()
        sqs = Mock()
        sqs.receive_message.return_value = {"ResponseMetadata": {"RetryAttempts": 2, "HTTPStatusCode": 200}}
        messages, elapsed = receive_factory(diag, sqs)("queue", 10, 0)
        self.assertEqual(messages, [])
        self.assertGreaterEqual(elapsed, 0)
        self.assertEqual(diag.snapshot()["empty_receives"], 1)
        self.assertFalse(diag.snapshot()["receive_in_progress"])
        self.assertEqual(diag.emit.call_args.kwargs["retries"], 2)
        self.assertIn("ApproximateReceiveCount", sqs.receive_message.call_args.kwargs["AttributeNames"])

    def test_receive_error_is_reraised(self):
        diag = QueueDiagnostics(True)
        diag.emit = Mock()
        sqs = Mock()
        sqs.receive_message.side_effect = TimeoutError("sensitive text")
        with self.assertRaises(TimeoutError):
            receive_factory(diag, sqs)("queue", 10, 0)
        self.assertEqual(diag.snapshot()["receive_errors"], 1)
        self.assertFalse(diag.snapshot()["receive_in_progress"])
        self.assertEqual(diag.emit.call_args.kwargs["error"], "TimeoutError")
        self.assertNotIn("sensitive text", str(diag.emit.call_args))

    def test_received_submission_timestamps(self):
        diag = QueueDiagnostics(True)
        diag.emit = Mock()
        sqs = Mock()
        sqs.receive_message.return_value = {"Messages": [{"MessageId": "m1", "Body": "secret"}]}
        messages, _ = receive_factory(diag, sqs)("queue", 10, 0)
        diag.submit(messages, .1, 10, .01)
        self.assertGreaterEqual(messages[0]["_diag_submitted"], messages[0]["_diag_received"])
        self.assertNotIn("secret", str(diag.emit.call_args_list))
        self.assertAlmostEqual(diag.emit.call_args.kwargs["deadline_overshoot_s"], .09)

    def test_disabled_does_not_change_receive_attributes(self):
        diag = QueueDiagnostics(False)
        sqs = Mock()
        sqs.receive_message.return_value = {"Messages": [{"MessageId": "m1"}]}
        messages, _ = receive_factory(diag, sqs)("queue", 10, 0)
        self.assertEqual(sqs.receive_message.call_args.kwargs["AttributeNames"], ["SentTimestamp"])
        self.assertNotIn("_diag_received", messages[0])
        diag.start()
        self.assertIsNone(diag.thread)

    def test_heartbeat_while_receive_is_in_progress(self):
        diag = QueueDiagnostics(True, .1)
        seen = threading.Event()

        def emit(event, **fields):
            if event == "heartbeat" and fields.get("receive_in_progress"):
                seen.set()

        diag.emit = emit
        diag.begin_receive(10, 0)
        diag.start()
        try:
            self.assertTrue(seen.wait(2))
        finally:
            diag.stop()
        self.assertFalse(diag.thread.is_alive())

    def test_execution_counter_restored_on_error(self):
        diag = QueueDiagnostics(True)
        diag.emit = Mock()

        @diag.execution("active_units", lambda cmd: [cmd])
        def fail(cmd):
            self.assertEqual(diag.snapshot()["active_units"], 1)
            raise ValueError()

        with self.assertRaises(ValueError):
            fail({"request_id": "r1"})
        self.assertEqual(diag.snapshot()["active_units"], 0)
        self.assertEqual(diag.snapshot()["execution_tasks_finished"], 1)


if __name__ == "__main__":
    unittest.main()
