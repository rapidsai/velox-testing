# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import aws_cluster
import summarize_results


def valid_config() -> dict[str, str]:
    return {
        "AWS_PROFILE": "",
        "AWS_REGION": "us-east-2",
        "AMI_ID": "ami-example",
        "SUBNET_ID": "subnet-example",
        "SECURITY_GROUP_ID": "sg-example",
        "IAM_INSTANCE_PROFILE": "benchmark-role",
        "COORDINATOR_INSTANCE_TYPE": "m7a.4xlarge",
        "WORKER_INSTANCE_TYPE": "m7a.4xlarge",
        "WORKER_COUNT": "8",
        "ROOT_VOLUME_GIB": "100",
        "VELOX_TESTING_REPOSITORY": "https://example.com/velox-testing.git",
        "VELOX_TESTING_FETCH_REF": "refs/heads/example",
        "VELOX_TESTING_REF": "d" * 40,
        "COORDINATOR_IMAGE": "example/coordinator@sha256:abc",
        "WORKER_IMAGE": "example/worker@sha256:def",
        "HIVE_METASTORE_S3_URI": "s3://example/metastore/",
        "SCHEMA_NAME": "tpch_sf1k",
        "RESULTS_S3_ROOT": "s3://example/results",
        "VCPU_PER_WORKER": "16",
        "TASK_MAX_DRIVERS_PER_TASK": "16",
        "HIVE_MAX_SPLIT_SIZE": "256MB",
        "HIVE_SPLIT_LOADER_CONCURRENCY": "32",
        "DYNAMIC_FILTERING_ENABLED": "false",
        "CPU_EXCHANGE_TUNING_ENABLED": "true",
        "COORDINATOR_HEAP_GIB": "16",
        "COORDINATOR_HEADROOM_GIB": "4",
        "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB": "12",
        "COORDINATOR_QUERY_MEMORY_PER_NODE_GIB": "10",
        "WORKER_SYSTEM_MEMORY_GIB": "50",
        "WORKER_QUERY_MEMORY_GIB": "44",
        "WORKER_MEMORY_LIMIT_GIB": "54",
        "WORKER_MEMORY_SHRINK_GIB": "16",
        "ASYNC_DATA_CACHE_ENABLED": "false",
        "ASYNC_CACHE_SSD_GIB": "0",
        "ASYNC_CACHE_SSD_PATH": "",
        "ASYNC_CACHE_NUM_SHARDS": "16",
        "OWNER": "tester",
        "EXPIRY_HOURS": "8",
        "SSM_READY_TIMEOUT_SECONDS": "900",
        "SSM_COMMAND_TIMEOUT_SECONDS": "21600",
        "WORKER_READY_TIMEOUT_SECONDS": "600",
        "PLACEMENT_GROUP": "",
    }


class EnvTest(unittest.TestCase):
    def test_load_env_handles_comments_and_quotes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.env"
            path.write_text("# comment\nA=one\nB='two words'\n\n")
            self.assertEqual(aws_cluster.load_env(path), {"A": "one", "B": "two words"})

    def test_load_env_rejects_non_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.env"
            path.write_text("not an assignment\n")
            with self.assertRaises(aws_cluster.ClusterError):
                aws_cluster.load_env(path)


class ClusterTest(unittest.TestCase):
    def make_cluster(self, config: dict[str, str] | None = None, workers: int = 8) -> aws_cluster.Cluster:
        return aws_cluster.Cluster(
            config or valid_config(),
            run_id="test-run",
            workers=workers,
            state_root=Path("/tmp/unused-state"),
            dry_run=True,
        )

    def test_validation_accepts_supported_shapes(self) -> None:
        for workers in (1, 2, 8, 16, 32):
            self.make_cluster(workers=workers).validate()

    def test_validation_accepts_role_specific_amis(self) -> None:
        config = valid_config()
        config["AMI_ID"] = ""
        config["COORDINATOR_AMI_ID"] = "ami-x86"
        config["WORKER_AMI_ID"] = "ami-arm"
        cluster = self.make_cluster(config)
        cluster.validate()
        self.assertEqual(cluster.resolve_ami("coordinator"), "ami-x86")
        self.assertEqual(cluster.resolve_ami("worker"), "ami-arm")

    def test_validation_rejects_unplanned_shape(self) -> None:
        with self.assertRaisesRegex(aws_cluster.ClusterError, "worker count"):
            self.make_cluster(workers=3).validate()

    def test_validation_rejects_unsafe_worker_memory(self) -> None:
        config = valid_config()
        config["WORKER_QUERY_MEMORY_GIB"] = "50"
        with self.assertRaisesRegex(aws_cluster.ClusterError, "must be less"):
            self.make_cluster(config).validate()

    def test_validation_rejects_coordinator_memory_larger_than_heap(self) -> None:
        config = valid_config()
        config["COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB"] = "13"
        with self.assertRaisesRegex(aws_cluster.ClusterError, "must be at least"):
            self.make_cluster(config).validate()

    def test_validation_requires_complete_harness_bundle_identity(self) -> None:
        config = valid_config()
        config["HARNESS_BUNDLE_S3_URI"] = "s3://example/harness.tar.gz"
        with self.assertRaisesRegex(aws_cluster.ClusterError, "set both"):
            self.make_cluster(config).validate()

    def test_launch_dry_run_is_two_idempotent_fleet_calls(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.make_cluster().launch()
        rendered = output.getvalue()
        self.assertEqual(rendered.count("ec2 run-instances"), 2)
        self.assertIn("--client-token test-run-coordinator", rendered)
        self.assertIn("--client-token test-run-worker", rendered)
        self.assertIn("--count 8", rendered)
        self.assertIn("HttpTokens=required", rendered)
        self.assertNotIn("terminate-instances", rendered)

    def test_tags_scope_resources_to_run(self) -> None:
        tags = {item["Key"]: item["Value"] for item in self.make_cluster().tags("worker", "2026-08-25T00:00:00+00:00")}
        self.assertEqual(tags["Project"], "cudf-performance")
        self.assertEqual(tags["Experiment"], "20260824_aws_102")
        self.assertEqual(tags["RunId"], "test-run")
        self.assertEqual(tags["Role"], "worker")

    def test_inventory_filters_by_owner_before_destructive_use(self) -> None:
        cluster = self.make_cluster()
        calls: list[list[str]] = []

        def fake_run(arguments: list[str], **_: object) -> dict[str, list[object]]:
            calls.append(arguments)
            return {"Reservations": []}

        cluster.aws.run = fake_run  # type: ignore[method-assign]
        self.assertEqual(cluster.inventory(), [])
        rendered = " ".join(calls[0])
        self.assertIn("Name=tag:RunId,Values=test-run", rendered)
        self.assertIn("Name=tag:Owner,Values=tester", rendered)

    def test_remote_env_contains_fixed_scale_rules(self) -> None:
        rendered = self.make_cluster(workers=16).remote_env("10.0.0.1", "worker", 3)
        self.assertIn("WORKER_COUNT=16", rendered)
        self.assertIn("WORKER_INDEX=3", rendered)
        self.assertIn("COORDINATOR_ADDRESS=10.0.0.1", rendered)
        self.assertIn("ASYNC_DATA_CACHE_ENABLED=false", rendered)
        self.assertIn("TASK_MAX_DRIVERS_PER_TASK=16", rendered)
        self.assertIn("HIVE_MAX_SPLIT_SIZE=256MB", rendered)

    def test_validation_rejects_invalid_cpu_tuning_values(self) -> None:
        config = valid_config()
        config["HIVE_MAX_SPLIT_SIZE"] = "256"
        with self.assertRaisesRegex(aws_cluster.ClusterError, "integer number of MB"):
            self.make_cluster(config).validate()

        config = valid_config()
        config["CPU_EXCHANGE_TUNING_ENABLED"] = "yes"
        with self.assertRaisesRegex(aws_cluster.ClusterError, "true or false"):
            self.make_cluster(config).validate()

    def test_ssm_dry_run_sets_long_execution_timeout(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.make_cluster().send_command(["i-example"], ["sleep 7200"], "long benchmark")
        rendered = output.getvalue()
        self.assertIn("executionTimeout", rendered)
        self.assertIn("21600", rendered)


class SummaryTest(unittest.TestCase):
    def write_result(self, directory: str, name: str, raw: dict[str, list[int]]) -> Path:
        path = Path(directory) / name
        path.write_text(json.dumps({"tpch": {"raw_times_ms": raw, "failed_queries": {}}}))
        return path

    def test_baseline_discards_first_iteration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_result(
                directory,
                "baseline.json",
                {"Q1": [100, 20, 30, 40, 50], "Q2": [200, 10, 20, 30, 40]},
            )
            summary = summarize_results.baseline(path)
            self.assertEqual(summary["per_query_mean_ms"], {"Q1": 35, "Q2": 25})
            self.assertEqual(summary["suite_runtime_ms"], 60)

    def test_cache_series_uses_four_suite_major_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = [
                self.write_result(
                    directory,
                    f"pass-{index}.json",
                    {"Q1": [100 - index * 10], "Q2": [200 - index * 20]},
                )
                for index in range(5)
            ]
            summary = summarize_results.cache_series(paths)
            self.assertEqual(summary["cold_fill"]["suite_runtime_ms"], 300)
            self.assertEqual(summary["warm_reuse"]["per_query_mean_ms"]["Q1"], 75)
            self.assertEqual(summary["warm_reuse"]["suite_runtime_ms"], 225)


if __name__ == "__main__":
    unittest.main()
