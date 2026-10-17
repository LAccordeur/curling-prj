"""Offline regression tests for independent object-store and queue credentials."""
import ast
import itertools
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]


def client_factory(service, env):
    # Extract only the factory to avoid importing DB engines and starting clients.
    path = ROOT / "services" / service
    tree = ast.parse((path / ("main.py" if service == "api" else "executor.py")).read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "boto3_client")
    with patch.dict(os.environ, env, clear=True):
        namespace = runpy.run_path(str(path / "config.py"))
    namespace.update(boto3=Mock(), cfg=object())
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class CredentialTests(unittest.TestCase):
    def test_backend_matrix(self):
        for service, obj, queue in itertools.product(
            ("api", "executor"), ("minio", "aws_s3"), ("elasticmq", "aws_sqs")
        ):
            with self.subTest(service=service, obj=obj, queue=queue):
                ns = client_factory(service, {
                    "OBJECT_STORE_BACKEND": obj, "QUEUE_BACKEND": queue,
                    "MINIO_ACCESS_KEY_ID": "local-key", "MINIO_SECRET_ACCESS_KEY": "local-secret",
                    "AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret",
                    "AWS_SESSION_TOKEN": "temporary-token",
                })
                for kind, local in (("s3", obj == "minio"), ("sqs", queue == "elasticmq")):
                    ns["boto3_client"](kind, "http://local" if local else None)
                    kwargs = ns["boto3"].client.call_args.kwargs
                    if local:
                        self.assertEqual(kwargs["aws_access_key_id"], "local-key" if kind == "s3" else "x")
                        self.assertNotIn("aws_session_token", kwargs)
                    else:
                        # Leave all AWS credentials to boto3, including the session token.
                        self.assertFalse(any(k.startswith("aws_") for k in kwargs))

    def test_missing_local_credentials_fail_clearly(self):
        for service in ("api", "executor"):
            ns = client_factory(service, {"AWS_ACCESS_KEY_ID": "old-minio-key",
                                          "AWS_SECRET_ACCESS_KEY": "old-minio-secret"})
            with self.assertRaisesRegex(ValueError, "regenerate env files"):
                ns["boto3_client"]("s3", "http://minio")
            with self.assertRaisesRegex(ValueError, "SQS_ENDPOINT_URL"):
                ns["boto3_client"]("sqs", None)

    def test_env_generators(self):
        for obj, queue, profile in itertools.product(
            ("minio", "aws_s3"), ("elasticmq", "aws_sqs"), ("", "curling")
        ):
            with self.subTest(obj=obj, queue=queue, profile=profile), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                for name in ("control-plane", "private", "exchange", "client", "scripts"):
                    (root / "deploy" / name).mkdir(parents=True)
                for file in ("create_cluster_envs.sh", "create_control_env.sh", "create_private_env.sh",
                             "deploy/scripts/domain_config.sh", "deploy/scripts/exchange_env.sh"):
                    shutil.copy(ROOT / file, root / file)
                inventory = (ROOT / "inventory.env.example").read_text()
                inventory += (
                    f"\nOBJECT_STORE_BACKEND={obj}\nQUEUE_BACKEND={queue}\n"
                    f"AWS_ACCOUNT_ID=123456789012\nAWS_PROFILE={profile}\n"
                    "MINIO_ACCESS_KEY_ID=local-key\nMINIO_SECRET_ACCESS_KEY=local-secret\n"
                    'PRIVATE_DOMAINS="domain-a domain-b"\n'
                )
                (root / "inventory.env").write_text(inventory)
                env = {"PATH": os.environ["PATH"], "HOME": tmp,
                       "AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret"}
                for script in ("create_cluster_envs.sh", "create_control_env.sh", "create_private_env.sh"):
                    subprocess.run(["bash", str(root / script)], env=env, check=True, capture_output=True)
                for relative in ("control-plane/.env", "private/.env", "private/domain-a.env",
                                 "private/domain-b.env", "exchange/.env"):
                    contents = (root / "deploy" / relative).read_text()
                    self.assertNotIn("AWS_ACCESS_KEY_ID=", contents)
                    self.assertNotIn("AWS_SECRET_ACCESS_KEY=", contents)
                    if profile:
                        self.assertIn(f"AWS_PROFILE={profile}", contents)
                    else:
                        self.assertNotIn("AWS_PROFILE=", contents)
                    if obj == "minio":
                        self.assertIn("MINIO_ACCESS_KEY_ID=local-key", contents)
                    if relative != "exchange/.env":
                        self.assertIn(f"QUEUE_BACKEND={queue}", contents)
                        if queue == "aws_sqs":
                            self.assertIn("SQS_ENDPOINT_URL=\n", contents)
                            self.assertIn("https://sqs.us-east-1.amazonaws.com/", contents)


if __name__ == "__main__":
    unittest.main()
