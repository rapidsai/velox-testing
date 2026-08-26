#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Manage an ephemeral Presto CPU benchmark fleet on direct EC2 instances."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_TAG = "cudf-performance"
DEFAULT_STATE_ROOT = Path.home() / ".cache" / "velox-testing" / "aws-ec2"


class ClusterError(RuntimeError):
    """A user-actionable cluster orchestration failure."""


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text().splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ClusterError(f"{path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key.replace("_", "").isalnum() or not key[0].isalpha():
            raise ClusterError(f"{path}:{line_number}: invalid key {key!r}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


def require(config: dict[str, str], *keys: str) -> None:
    missing = [key for key in keys if not config.get(key)]
    if missing:
        raise ClusterError(f"missing required configuration: {', '.join(missing)}")


def positive_int(config: dict[str, str], key: str) -> int:
    try:
        value = int(config[key])
    except (KeyError, ValueError) as error:
        raise ClusterError(f"{key} must be an integer") from error
    if value <= 0:
        raise ClusterError(f"{key} must be greater than zero")
    return value


def nonnegative_int(config: dict[str, str], key: str) -> int:
    try:
        value = int(config[key])
    except (KeyError, ValueError) as error:
        raise ClusterError(f"{key} must be an integer") from error
    if value < 0:
        raise ClusterError(f"{key} must be greater than or equal to zero")
    return value


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def shell_join(parts: list[str]) -> str:
    return shlex.join(parts)


@dataclass
class Instance:
    instance_id: str
    role: str
    state: str
    private_ip: str
    private_dns: str
    instance_type: str
    launch_time: str


class Aws:
    def __init__(self, config: dict[str, str], dry_run: bool = False):
        self.config = config
        self.dry_run = dry_run

    def base(self, region: str | None = None) -> list[str]:
        command = ["aws"]
        if self.config.get("AWS_PROFILE"):
            command += ["--profile", self.config["AWS_PROFILE"]]
        command += ["--region", region or self.config["AWS_REGION"]]
        return command

    def run(
        self,
        service_args: list[str],
        *,
        json_output: bool = False,
        allow_dry_run: bool = True,
        region: str | None = None,
    ) -> Any:
        command = self.base(region=region) + service_args
        if self.dry_run and allow_dry_run:
            print(f"DRY-RUN {shell_join(command)}")
            return {} if json_output else ""
        if json_output:
            command += ["--output", "json"]
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise ClusterError(f"command failed: {shell_join(command)}\n{detail}")
        if json_output:
            return json.loads(completed.stdout or "{}")
        return completed.stdout


class Cluster:
    def __init__(
        self,
        config: dict[str, str],
        run_id: str,
        workers: int,
        state_root: Path,
        dry_run: bool,
    ):
        self.config = config
        self.run_id = run_id
        self.workers = workers
        self.state_dir = state_root / run_id
        self.aws = Aws(config, dry_run=dry_run)
        self.dry_run = dry_run

    def validate(self, cloud: bool = True) -> None:
        require(
            self.config,
            "AWS_REGION",
            "COORDINATOR_INSTANCE_TYPE",
            "WORKER_INSTANCE_TYPE",
            "ENGINE_VARIANT",
            "VELOX_TESTING_REPOSITORY",
            "VELOX_TESTING_FETCH_REF",
            "VELOX_TESTING_REF",
            "COORDINATOR_IMAGE",
            "WORKER_IMAGE",
            "HIVE_METASTORE_S3_URI",
            "SCHEMA_NAME",
            "OWNER",
            "EXPERIMENT_TAG",
            "HIVE_MAX_SPLIT_SIZE",
            "DYNAMIC_FILTERING_ENABLED",
            "CPU_EXCHANGE_TUNING_ENABLED",
            "GPU_DEVICE_ID",
            "GPU_BATCH_SIZE_MIN_THRESHOLD",
            "GPU_USE_BUFFERED_INPUT",
            "GPU_USE_KVIKIO",
            "KVIKIO_REMOTE_IO_BACKEND",
            "CUDA_MODULE_LOADING",
            "ASYNC_CACHE_SSD_GIB",
            "ASYNC_CACHE_NUM_SHARDS",
        )
        if cloud:
            require(
                self.config,
                "SUBNET_ID",
                "SECURITY_GROUP_ID",
                "IAM_INSTANCE_PROFILE",
            )
            generic_ami = self.config.get("AMI_ID") or self.config.get("AMI_SSM_PARAMETER")
            coordinator_ami = self.config.get("COORDINATOR_AMI_ID") or self.config.get(
                "COORDINATOR_AMI_SSM_PARAMETER"
            )
            worker_ami = self.config.get("WORKER_AMI_ID") or self.config.get(
                "WORKER_AMI_SSM_PARAMETER"
            )
            if not generic_ami and not (coordinator_ami and worker_ami):
                raise ClusterError(
                    "set AMI_ID/AMI_SSM_PARAMETER or role-specific coordinator and worker AMIs"
                )
        for key in (
            "ROOT_VOLUME_GIB",
            "EXPIRY_HOURS",
            "SSM_READY_TIMEOUT_SECONDS",
            "SSM_COMMAND_TIMEOUT_SECONDS",
            "WORKER_READY_TIMEOUT_SECONDS",
            "VCPU_PER_WORKER",
            "TASK_MAX_DRIVERS_PER_TASK",
            "HIVE_SPLIT_LOADER_CONCURRENCY",
            "KVIKIO_NTHREADS",
            "KVIKIO_TASK_SIZE",
            "KVIKIO_BOUNCE_BUFFER_SIZE",
            "KVIKIO_REMOTE_IO_NUM_REACTORS",
            "KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS",
            "LIBCUDF_NUM_HOST_WORKERS",
            "COORDINATOR_HEAP_GIB",
            "COORDINATOR_HEADROOM_GIB",
            "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB",
            "COORDINATOR_QUERY_MEMORY_PER_NODE_GIB",
            "WORKER_SYSTEM_MEMORY_GIB",
            "WORKER_QUERY_MEMORY_GIB",
            "WORKER_MEMORY_LIMIT_GIB",
            "WORKER_MEMORY_SHRINK_GIB",
        ):
            positive_int(self.config, key)
        if self.workers not in (1, 2, 8, 16, 32):
            raise ClusterError("worker count must be one of: 1, 2, 8, 16, 32")
        if not self.run_id.replace("-", "").replace("_", "").isalnum():
            raise ClusterError("RunId may contain only letters, digits, '-' and '_'")
        source_ref = self.config["VELOX_TESTING_REF"]
        if len(source_ref) != 40 or any(character not in "0123456789abcdefABCDEF" for character in source_ref):
            raise ClusterError("VELOX_TESTING_REF must be a full 40-character commit SHA")
        bundle_uri = self.config.get("HARNESS_BUNDLE_S3_URI", "")
        bundle_sha = self.config.get("HARNESS_BUNDLE_SHA256", "")
        if bool(bundle_uri) != bool(bundle_sha):
            raise ClusterError("set both HARNESS_BUNDLE_S3_URI and HARNESS_BUNDLE_SHA256, or neither")
        if bundle_sha and (
            len(bundle_sha) != 64 or any(character not in "0123456789abcdefABCDEF" for character in bundle_sha)
        ):
            raise ClusterError("HARNESS_BUNDLE_SHA256 must contain 64 hexadecimal characters")
        if self.config["ASYNC_DATA_CACHE_ENABLED"] not in ("true", "false"):
            raise ClusterError("ASYNC_DATA_CACHE_ENABLED must be true or false")
        if self.config["ENGINE_VARIANT"] not in ("cpu", "gpu"):
            raise ClusterError("ENGINE_VARIANT must be cpu or gpu")
        if self.config["ENGINE_VARIANT"] == "gpu":
            nonnegative_int(self.config, "GPU_DEVICE_ID")
            positive_int(self.config, "GPU_BATCH_SIZE_MIN_THRESHOLD")
        for key in (
            "DYNAMIC_FILTERING_ENABLED",
            "CPU_EXCHANGE_TUNING_ENABLED",
            "GPU_USE_BUFFERED_INPUT",
            "GPU_USE_KVIKIO",
        ):
            if self.config[key] not in ("true", "false"):
                raise ClusterError(f"{key} must be true or false")
        split_size = self.config["HIVE_MAX_SPLIT_SIZE"]
        if not split_size.endswith("MB") or not split_size[:-2].isdigit():
            raise ClusterError("HIVE_MAX_SPLIT_SIZE must be an integer number of MB")
        if int(split_size[:-2]) <= 0:
            raise ClusterError("HIVE_MAX_SPLIT_SIZE must be greater than zero")
        ssd_gib = nonnegative_int(self.config, "ASYNC_CACHE_SSD_GIB")
        positive_int(self.config, "ASYNC_CACHE_NUM_SHARDS")
        if ssd_gib and not self.config.get("ASYNC_CACHE_SSD_PATH"):
            raise ClusterError("ASYNC_CACHE_SSD_PATH is required when ASYNC_CACHE_SSD_GIB is nonzero")
        if ssd_gib and not self.config["ASYNC_CACHE_SSD_PATH"].startswith("/mnt/nvme/"):
            raise ClusterError("ASYNC_CACHE_SSD_PATH must be under /mnt/nvme/")
        if positive_int(self.config, "WORKER_QUERY_MEMORY_GIB") >= positive_int(
            self.config, "WORKER_SYSTEM_MEMORY_GIB"
        ):
            raise ClusterError("WORKER_QUERY_MEMORY_GIB must be less than WORKER_SYSTEM_MEMORY_GIB")
        if positive_int(self.config, "COORDINATOR_QUERY_MEMORY_PER_NODE_GIB") >= positive_int(
            self.config, "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB"
        ):
            raise ClusterError(
                "COORDINATOR_QUERY_MEMORY_PER_NODE_GIB must be less than COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB"
            )
        coordinator_required_heap = positive_int(
            self.config, "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB"
        ) + positive_int(self.config, "COORDINATOR_HEADROOM_GIB")
        if coordinator_required_heap > positive_int(self.config, "COORDINATOR_HEAP_GIB"):
            raise ClusterError(
                "COORDINATOR_HEAP_GIB must be at least "
                "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB + COORDINATOR_HEADROOM_GIB"
            )

    def tags(self, role: str, expiry: str) -> list[dict[str, str]]:
        engine = self.config["ENGINE_VARIANT"]
        return [
            {"Key": "Name", "Value": f"presto-{engine}-{role}-{self.run_id}"},
            {"Key": "Project", "Value": PROJECT_TAG},
            {"Key": "Experiment", "Value": self.config["EXPERIMENT_TAG"]},
            {"Key": "RunId", "Value": self.run_id},
            {"Key": "Role", "Value": role},
            {"Key": "Owner", "Value": self.config["OWNER"]},
            {"Key": "ExpiresAt", "Value": expiry},
        ]

    def resolve_ami(self, role: str) -> str:
        role_prefix = role.upper()
        ami_id = self.config.get(f"{role_prefix}_AMI_ID") or self.config.get("AMI_ID")
        if ami_id:
            return ami_id
        parameter = self.config.get(f"{role_prefix}_AMI_SSM_PARAMETER") or self.config.get(
            "AMI_SSM_PARAMETER"
        )
        if not parameter:
            raise ClusterError(f"no AMI configured for {role}")
        response = self.aws.run(
            [
                "ssm",
                "get-parameter",
                "--name",
                parameter,
            ],
            json_output=True,
        )
        return "ami-dry-run" if self.dry_run else response["Parameter"]["Value"]

    def resolve_hourly_price(self, instance_type: str) -> float | None:
        filters = [
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {
                "Type": "TERM_MATCH",
                "Field": "regionCode",
                "Value": self.config["AWS_REGION"],
            },
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
        ]
        response = self.aws.run(
            [
                "pricing",
                "get-products",
                "--service-code",
                "AmazonEC2",
                "--filters",
                json.dumps(filters, separators=(",", ":")),
                "--max-results",
                "100",
            ],
            json_output=True,
            region="us-east-1",
        )
        if self.dry_run:
            return None
        prices: set[float] = set()
        for encoded_product in response.get("PriceList", []):
            product = json.loads(encoded_product)
            for term in product.get("terms", {}).get("OnDemand", {}).values():
                for dimension in term.get("priceDimensions", {}).values():
                    usd = dimension.get("pricePerUnit", {}).get("USD")
                    if usd is not None:
                        prices.add(float(usd))
        nonzero = sorted(price for price in prices if price > 0)
        if len(nonzero) != 1:
            raise ClusterError(
                f"expected one on-demand Linux price for {instance_type} in "
                f"{self.config['AWS_REGION']}; found {nonzero}"
            )
        return nonzero[0]

    def launch_role(self, role: str, count: int, expiry: str, ami_id: str) -> list[str]:
        instance_type = self.config["COORDINATOR_INSTANCE_TYPE" if role == "coordinator" else "WORKER_INSTANCE_TYPE"]
        mappings = [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": positive_int(self.config, "ROOT_VOLUME_GIB"),
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ]
        tag_specifications = [
            {"ResourceType": "instance", "Tags": self.tags(role, expiry)},
            {"ResourceType": "volume", "Tags": self.tags(role, expiry)},
        ]
        args = [
            "ec2",
            "run-instances",
            "--image-id",
            ami_id,
            "--instance-type",
            instance_type,
            "--count",
            str(count),
            "--subnet-id",
            self.config["SUBNET_ID"],
            "--security-group-ids",
            self.config["SECURITY_GROUP_ID"],
            "--iam-instance-profile",
            f"Name={self.config['IAM_INSTANCE_PROFILE']}",
            "--metadata-options",
            "HttpTokens=required,HttpEndpoint=enabled,HttpPutResponseHopLimit=2",
            "--block-device-mappings",
            json.dumps(mappings, separators=(",", ":")),
            "--instance-initiated-shutdown-behavior",
            "terminate",
            "--user-data",
            (f"#!/usr/bin/env bash\nshutdown -h +{positive_int(self.config, 'EXPIRY_HOURS') * 60}\n"),
            "--client-token",
            f"{self.run_id}-{role}",
            "--tag-specifications",
            json.dumps(tag_specifications, separators=(",", ":")),
        ]
        if self.config.get("PLACEMENT_GROUP"):
            args += ["--placement", f"GroupName={self.config['PLACEMENT_GROUP']}"]
        response = self.aws.run(args, json_output=True)
        if self.dry_run:
            return []
        return [item["InstanceId"] for item in response["Instances"]]

    def launch(self) -> None:
        self.validate(cloud=True)
        expiry = (utc_now() + dt.timedelta(hours=positive_int(self.config, "EXPIRY_HOURS"))).isoformat()
        coordinator_ami_id = self.resolve_ami("coordinator")
        worker_ami_id = self.resolve_ami("worker")
        coordinator_price = self.resolve_hourly_price(self.config["COORDINATOR_INSTANCE_TYPE"])
        worker_price = self.resolve_hourly_price(self.config["WORKER_INSTANCE_TYPE"])
        coordinator_ids: list[str] = []
        worker_ids: list[str] = []
        try:
            coordinator_ids = self.launch_role("coordinator", 1, expiry, coordinator_ami_id)
            worker_ids = self.launch_role("worker", self.workers, expiry, worker_ami_id)
            if self.dry_run:
                return
            instance_ids = coordinator_ids + worker_ids
            self.aws.run(["ec2", "wait", "instance-running", "--instance-ids", *instance_ids])
            self.aws.run(["ec2", "wait", "instance-status-ok", "--instance-ids", *instance_ids])
            self.write_manifest(
                {
                    "run_id": self.run_id,
                    "created_at": utc_now().isoformat(),
                    "expires_at": expiry,
                    "worker_count": self.workers,
                    "coordinator_ami_id": coordinator_ami_id,
                    "worker_ami_id": worker_ami_id,
                    "coordinator_instance_ids": coordinator_ids,
                    "worker_instance_ids": worker_ids,
                    "coordinator_hourly_price_usd": coordinator_price,
                    "worker_hourly_price_usd": worker_price,
                    "source_ref": self.config["VELOX_TESTING_REF"],
                    "coordinator_image": self.config["COORDINATOR_IMAGE"],
                    "worker_image": self.config["WORKER_IMAGE"],
                    "harness_bundle_s3_uri": self.config.get("HARNESS_BUNDLE_S3_URI", ""),
                    "harness_bundle_sha256": self.config.get("HARNESS_BUNDLE_SHA256", ""),
                    "async_data_cache": self.config["ASYNC_DATA_CACHE_ENABLED"] == "true",
                    "engine_variant": self.config["ENGINE_VARIANT"],
                }
            )
            self.wait_for_ssm(instance_ids)
            print(json.dumps(self.inventory_json(), indent=2))
        except (ClusterError, OSError):
            if not self.dry_run:
                try:
                    self.terminate()
                except (ClusterError, OSError) as cleanup_error:
                    print(
                        f"warning: launch rollback failed: {cleanup_error}",
                        file=sys.stderr,
                    )
            raise

    def inventory_json(self) -> list[dict[str, str]]:
        return [item.__dict__ for item in self.inventory()]

    def inventory(self) -> list[Instance]:
        response = self.aws.run(
            [
                "ec2",
                "describe-instances",
                "--filters",
                f"Name=tag:Project,Values={PROJECT_TAG}",
                f"Name=tag:Experiment,Values={self.config['EXPERIMENT_TAG']}",
                f"Name=tag:RunId,Values={self.run_id}",
                f"Name=tag:Owner,Values={self.config['OWNER']}",
                "Name=instance-state-name,Values=pending,running,stopping,stopped",
            ],
            json_output=True,
            allow_dry_run=False,
        )
        instances: list[Instance] = []
        for reservation in response.get("Reservations", []):
            for raw in reservation.get("Instances", []):
                tags = {tag["Key"]: tag["Value"] for tag in raw.get("Tags", [])}
                instances.append(
                    Instance(
                        instance_id=raw["InstanceId"],
                        role=tags.get("Role", "unknown"),
                        state=raw["State"]["Name"],
                        private_ip=raw.get("PrivateIpAddress", ""),
                        private_dns=raw.get("PrivateDnsName", ""),
                        instance_type=raw["InstanceType"],
                        launch_time=str(raw["LaunchTime"]),
                    )
                )
        return sorted(instances, key=lambda item: (item.role, item.instance_id))

    def expected_inventory(self) -> tuple[Instance, list[Instance]]:
        items = self.inventory()
        coordinators = [item for item in items if item.role == "coordinator"]
        workers = [item for item in items if item.role == "worker"]
        if len(coordinators) != 1 or len(workers) != self.workers:
            raise ClusterError(
                f"RunId {self.run_id}: expected 1 coordinator and {self.workers} "
                f"workers; found {len(coordinators)} and {len(workers)}"
            )
        non_running = [item.instance_id for item in items if item.state != "running"]
        if non_running:
            raise ClusterError(f"instances are not running: {', '.join(non_running)}")
        return coordinators[0], workers

    def wait_for_ssm(self, instance_ids: list[str]) -> None:
        deadline = time.monotonic() + positive_int(self.config, "SSM_READY_TIMEOUT_SECONDS")
        while time.monotonic() < deadline:
            response = self.aws.run(
                [
                    "ssm",
                    "describe-instance-information",
                    "--filters",
                    "Key=InstanceIds,Values=" + ",".join(instance_ids),
                ],
                json_output=True,
            )
            online = {
                item["InstanceId"]
                for item in response.get("InstanceInformationList", [])
                if item.get("PingStatus") == "Online"
            }
            if online == set(instance_ids):
                return
            time.sleep(10)
        missing = sorted(set(instance_ids) - online)
        raise ClusterError(f"instances did not become SSM Online: {', '.join(missing)}")

    def send_command(
        self,
        instance_ids: list[str],
        commands: list[str],
        comment: str,
        *,
        wait: bool = True,
    ) -> str:
        execution_timeout = positive_int(self.config, "SSM_COMMAND_TIMEOUT_SECONDS")
        parameters = json.dumps(
            {
                "commands": commands,
                "executionTimeout": [str(execution_timeout)],
            },
            separators=(",", ":"),
        )
        args = [
            "ssm",
            "send-command",
            "--document-name",
            "AWS-RunShellScript",
            "--instance-ids",
            *instance_ids,
            "--comment",
            comment[:100],
            "--parameters",
            parameters,
        ]
        if self.config.get("RESULTS_S3_ROOT"):
            parsed = self.config["RESULTS_S3_ROOT"].removeprefix("s3://")
            bucket, _, prefix = parsed.partition("/")
            args += ["--output-s3-bucket-name", bucket]
            if prefix:
                args += [
                    "--output-s3-key-prefix",
                    f"{prefix.rstrip('/')}/{self.run_id}/ssm",
                ]
        response = self.aws.run(args, json_output=True)
        if self.dry_run:
            return "dry-run-command"
        command_id = response["Command"]["CommandId"]
        if wait:
            for instance_id in instance_ids:
                self.wait_for_command(
                    command_id,
                    instance_id,
                    timeout_seconds=execution_timeout + 300,
                )
        return command_id

    def wait_for_command(self, command_id: str, instance_id: str, timeout_seconds: int) -> None:
        deadline = time.monotonic() + timeout_seconds
        terminal = {"Success", "Cancelled", "TimedOut", "Failed", "Cancelling"}
        while time.monotonic() < deadline:
            response = self.aws.run(
                [
                    "ssm",
                    "list-command-invocations",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                    "--details",
                ],
                json_output=True,
            )
            invocations = response.get("CommandInvocations", [])
            if not invocations:
                time.sleep(5)
                continue
            status = invocations[0].get("Status", "")
            if status not in terminal:
                time.sleep(5)
                continue
            if status == "Success":
                return
            invocation = self.aws.run(
                [
                    "ssm",
                    "get-command-invocation",
                    "--command-id",
                    command_id,
                    "--instance-id",
                    instance_id,
                ],
                json_output=True,
            )
            raise ClusterError(
                f"SSM command {command_id} ended as {status} on {instance_id}: "
                f"{invocation.get('StandardErrorContent', '').strip()}"
            )
        raise ClusterError(
            f"timed out waiting {timeout_seconds}s for SSM command {command_id} "
            f"on {instance_id}; the remote command may still be running"
        )

    def bootstrap(self) -> None:
        self.validate(cloud=True)
        coordinator, workers = self.expected_inventory()
        repository = shlex.quote(self.config["VELOX_TESTING_REPOSITORY"])
        fetch_ref = shlex.quote(self.config["VELOX_TESTING_FETCH_REF"])
        ref = shlex.quote(self.config["VELOX_TESTING_REF"])
        commands = [
            "set -eu",
            "export DEBIAN_FRONTEND=noninteractive",
            "apt-get -o DPkg::Lock::Timeout=300 update -y",
            (
                "apt-get -o DPkg::Lock::Timeout=300 install -y "
                "ca-certificates curl git iproute2 jq numactl "
                "python3 python3-venv unzip"
            ),
            (
                "if ! command -v docker >/dev/null; then "
                "apt-get -o DPkg::Lock::Timeout=300 install -y docker.io; fi"
            ),
            "systemctl enable --now docker",
            (
                "if ! command -v aws >/dev/null; then "
                "case \"$(uname -m)\" in "
                "x86_64) aws_arch=x86_64 ;; "
                "aarch64|arm64) aws_arch=aarch64 ;; "
                "*) echo 'unsupported architecture for AWS CLI' >&2; exit 1 ;; "
                "esac; "
                "curl -fsSLo /tmp/awscliv2.zip "
                "\"https://awscli.amazonaws.com/awscli-exe-linux-${aws_arch}.zip\"; "
                "rm -rf /tmp/aws; unzip -q /tmp/awscliv2.zip -d /tmp; "
                "/tmp/aws/install; fi"
            ),
            "rm -rf /opt/velox-testing",
            f"git clone --filter=blob:none {repository} /opt/velox-testing",
            f"git -C /opt/velox-testing fetch origin {fetch_ref}",
            (f'test "$(git -C /opt/velox-testing rev-parse FETCH_HEAD)" = {ref}'),
            f"git -C /opt/velox-testing checkout --detach {ref}",
        ]
        if self.config.get("HARNESS_BUNDLE_S3_URI"):
            bundle_uri = shlex.quote(self.config["HARNESS_BUNDLE_S3_URI"])
            bundle_sha = shlex.quote(self.config["HARNESS_BUNDLE_SHA256"])
            commands += [
                f"aws s3 cp {bundle_uri} /tmp/presto-aws-harness.tar.gz --only-show-errors",
                (f"echo '{bundle_sha}  /tmp/presto-aws-harness.tar.gz' | sha256sum --check --strict"),
                "tar -xzf /tmp/presto-aws-harness.tar.gz -C /opt/velox-testing",
            ]
        bootstrap_prefix = (
            "bash /opt/velox-testing/presto/aws/ec2/remote/bootstrap_node.sh "
        )
        bootstrap_suffix = (
            shlex.quote(self.config["COORDINATOR_IMAGE"])
            + " "
            + shlex.quote(self.config["WORKER_IMAGE"])
            + " "
            + shlex.quote(self.config["HIVE_METASTORE_S3_URI"])
            + " "
            + shlex.quote(self.config["ENGINE_VARIANT"])
        )
        coordinator_command_id = self.send_command(
            [coordinator.instance_id],
            [*commands, f"{bootstrap_prefix}coordinator {bootstrap_suffix}"],
            f"bootstrap coordinator {self.run_id}",
        )
        self.record_command("bootstrap_coordinator", coordinator_command_id)
        worker_command_id = self.send_command(
            [item.instance_id for item in workers],
            [*commands, f"{bootstrap_prefix}worker {bootstrap_suffix}"],
            f"bootstrap workers {self.run_id}",
        )
        self.record_command("bootstrap_workers", worker_command_id)

    def remote_env(self, coordinator_address: str, role: str, worker_index: int | None = None) -> str:
        values = {
            "RUN_ID": self.run_id,
            "ROLE": role,
            "WORKER_COUNT": str(self.workers),
            "WORKER_INDEX": "" if worker_index is None else str(worker_index),
            "COORDINATOR_ADDRESS": coordinator_address,
            "AWS_REGION": self.config["AWS_REGION"],
            "ENGINE_VARIANT": self.config["ENGINE_VARIANT"],
            "VCPU_PER_WORKER": self.config["VCPU_PER_WORKER"],
            "TASK_MAX_DRIVERS_PER_TASK": self.config["TASK_MAX_DRIVERS_PER_TASK"],
            "HIVE_MAX_SPLIT_SIZE": self.config["HIVE_MAX_SPLIT_SIZE"],
            "HIVE_SPLIT_LOADER_CONCURRENCY": self.config[
                "HIVE_SPLIT_LOADER_CONCURRENCY"
            ],
            "DYNAMIC_FILTERING_ENABLED": self.config["DYNAMIC_FILTERING_ENABLED"],
            "CPU_EXCHANGE_TUNING_ENABLED": self.config[
                "CPU_EXCHANGE_TUNING_ENABLED"
            ],
            "GPU_DEVICE_ID": self.config["GPU_DEVICE_ID"],
            "GPU_BATCH_SIZE_MIN_THRESHOLD": self.config[
                "GPU_BATCH_SIZE_MIN_THRESHOLD"
            ],
            "GPU_PARTITIONED_OUTPUT_BATCH_ROWS": self.config.get(
                "GPU_PARTITIONED_OUTPUT_BATCH_ROWS", "100000000"
            ),
            "GPU_UCXX_BLOCKING_POLLING": self.config.get(
                "GPU_UCXX_BLOCKING_POLLING", "true"
            ),
            "GPU_UCX_NET_DEVICES": self.config.get("GPU_UCX_NET_DEVICES", "all"),
            "GPU_UCX_TCP_TX_SEG_SIZE": self.config.get(
                "GPU_UCX_TCP_TX_SEG_SIZE", "8K"
            ),
            "GPU_UCX_TCP_RX_SEG_SIZE": self.config.get(
                "GPU_UCX_TCP_RX_SEG_SIZE", "64K"
            ),
            "GPU_UCX_RNDV_FRAG_SIZE": self.config.get(
                "GPU_UCX_RNDV_FRAG_SIZE", "host:512K,cuda:4M"
            ),
            "GPU_UCX_CUDA_COPY_MAX_REG_RATIO": self.config.get(
                "GPU_UCX_CUDA_COPY_MAX_REG_RATIO", "0.100"
            ),
            "GPU_UCX_TCP_MAX_BW": self.config.get(
                "GPU_UCX_TCP_MAX_BW", "2200MBps"
            ),
            "GPU_UCX_TCP_MAX_POLL": self.config.get(
                "GPU_UCX_TCP_MAX_POLL", "16"
            ),
            "GPU_UCX_CUDA_COPY_MAX_POLL": self.config.get(
                "GPU_UCX_CUDA_COPY_MAX_POLL", "16"
            ),
            "GPU_USE_BUFFERED_INPUT": self.config["GPU_USE_BUFFERED_INPUT"],
            "GPU_USE_KVIKIO": self.config["GPU_USE_KVIKIO"],
            "KVIKIO_REMOTE_IO_BACKEND": self.config["KVIKIO_REMOTE_IO_BACKEND"],
            "KVIKIO_NTHREADS": self.config["KVIKIO_NTHREADS"],
            "KVIKIO_TASK_SIZE": self.config["KVIKIO_TASK_SIZE"],
            "KVIKIO_BOUNCE_BUFFER_SIZE": self.config[
                "KVIKIO_BOUNCE_BUFFER_SIZE"
            ],
            "KVIKIO_REMOTE_IO_NUM_REACTORS": self.config[
                "KVIKIO_REMOTE_IO_NUM_REACTORS"
            ],
            "KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS": self.config[
                "KVIKIO_REMOTE_IO_MAX_CONCURRENT_REQUESTS"
            ],
            "LIBCUDF_NUM_HOST_WORKERS": self.config["LIBCUDF_NUM_HOST_WORKERS"],
            "CUDA_MODULE_LOADING": self.config["CUDA_MODULE_LOADING"],
            "COORDINATOR_HEAP_GIB": self.config["COORDINATOR_HEAP_GIB"],
            "COORDINATOR_HEADROOM_GIB": self.config["COORDINATOR_HEADROOM_GIB"],
            "COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB": self.config["COORDINATOR_QUERY_TOTAL_MEMORY_PER_NODE_GIB"],
            "COORDINATOR_QUERY_MEMORY_PER_NODE_GIB": self.config["COORDINATOR_QUERY_MEMORY_PER_NODE_GIB"],
            "WORKER_SYSTEM_MEMORY_GIB": self.config["WORKER_SYSTEM_MEMORY_GIB"],
            "WORKER_QUERY_MEMORY_GIB": self.config["WORKER_QUERY_MEMORY_GIB"],
            "WORKER_MEMORY_LIMIT_GIB": self.config["WORKER_MEMORY_LIMIT_GIB"],
            "WORKER_MEMORY_SHRINK_GIB": self.config["WORKER_MEMORY_SHRINK_GIB"],
            "ASYNC_DATA_CACHE_ENABLED": self.config["ASYNC_DATA_CACHE_ENABLED"],
            "ASYNC_CACHE_SSD_GIB": self.config["ASYNC_CACHE_SSD_GIB"],
            "ASYNC_CACHE_SSD_PATH": self.config.get("ASYNC_CACHE_SSD_PATH", ""),
            "ASYNC_CACHE_NUM_SHARDS": self.config["ASYNC_CACHE_NUM_SHARDS"],
            "COORDINATOR_IMAGE": self.config["COORDINATOR_IMAGE"],
            "WORKER_IMAGE": self.config["WORKER_IMAGE"],
        }
        return " ".join(f"{key}={shlex.quote(value)}" for key, value in values.items())

    def start(self) -> None:
        self.validate(cloud=True)
        coordinator, workers = self.expected_inventory()
        address = coordinator.private_ip or coordinator.private_dns
        if nonnegative_int(self.config, "ASYNC_CACHE_SSD_GIB"):
            cache_path = shlex.quote(self.config["ASYNC_CACHE_SSD_PATH"])
            prepare_command_id = self.send_command(
                [worker.instance_id for worker in workers],
                [
                    "set -eu",
                    "mkdir -p /mnt/nvme",
                    (
                        "if mountpoint -q /opt/dlami/nvme; then "
                        "mountpoint -q /mnt/nvme || "
                        "mount --bind /opt/dlami/nvme /mnt/nvme; "
                        "else "
                        "device=$(lsblk -dpno NAME,MODEL | "
                        "awk '$0 ~ /Amazon EC2 NVMe Instance Storage/ "
                        "{print $1; exit}'); "
                        "test -n \"$device\"; test -b \"$device\"; "
                        "existing_mount=$(lsblk -nrpo MOUNTPOINT \"$device\" | "
                        "awk 'NF {print; exit}'); "
                        "if test -n \"$existing_mount\"; then "
                        "mountpoint -q /mnt/nvme || "
                        "mount --bind \"$existing_mount\" /mnt/nvme; "
                        "else "
                        "fstype=$(blkid -s TYPE -o value \"$device\" || true); "
                        "test \"$fstype\" != LVM2_member; "
                        "if test -z \"$fstype\"; then mkfs.ext4 -F \"$device\"; fi; "
                        "mountpoint -q /mnt/nvme || mount \"$device\" /mnt/nvme; "
                        "fi; "
                        "fi"
                    ),
                    f"mkdir -p {cache_path}",
                    f"chmod 0777 /mnt/nvme {cache_path}",
                ],
                f"prepare NVMe cache {self.run_id}",
                wait=True,
            )
            self.record_command("prepare_nvme_cache", prepare_command_id)
        command_id = self.send_command(
            [coordinator.instance_id],
            [
                "set -eu",
                (
                    f"{self.remote_env(address, 'coordinator')} "
                    "bash /opt/velox-testing/presto/aws/ec2/remote/configure_and_start.sh"
                ),
            ],
            f"start coordinator {self.run_id}",
        )
        self.record_command("start_coordinator", command_id)
        for index, worker in enumerate(workers):
            command_id = self.send_command(
                [worker.instance_id],
                [
                    "set -eu",
                    (
                        f"{self.remote_env(address, 'worker', index)} "
                        "bash /opt/velox-testing/presto/aws/ec2/remote/configure_and_start.sh"
                    ),
                ],
                f"start worker {index} {self.run_id}",
            )
            self.record_command(f"start_worker_{index}", command_id)
        timeout = self.config["WORKER_READY_TIMEOUT_SECONDS"]
        command_id = self.send_command(
            [coordinator.instance_id],
            [
                "set -eu",
                (
                    "bash /opt/velox-testing/presto/aws/ec2/remote/"
                    f"wait_for_cluster.sh {shlex.quote(address)} {self.workers} {timeout}"
                ),
            ],
            f"wait for workers {self.run_id}",
        )
        self.record_command("wait_for_cluster", command_id)

    def run_benchmark(
        self,
        queries: str,
        iterations: int,
        tag: str,
        record_name: str = "benchmark",
    ) -> None:
        self.validate(cloud=True)
        coordinator, _ = self.expected_inventory()
        address = coordinator.private_ip or coordinator.private_dns
        result_uri = self.result_uri()
        commands = [
            "set -eu",
            (
                f"AWS_DEFAULT_REGION={shlex.quote(self.config['AWS_REGION'])} "
                f"AWS_REGION={shlex.quote(self.config['AWS_REGION'])} "
                "HOME=/root "
                "bash /opt/velox-testing/presto/aws/ec2/remote/run_benchmark.sh "
                f"{shlex.quote(address)} {shlex.quote(self.config['SCHEMA_NAME'])} "
                f"{shlex.quote(queries)} {iterations} {shlex.quote(tag)} "
                f"{shlex.quote(result_uri)}"
            ),
        ]
        command_id = self.send_command([coordinator.instance_id], commands, f"benchmark {self.run_id}", wait=True)
        self.record_command(record_name, command_id)

    def verify_gpu_exchange(self) -> None:
        self.validate(cloud=True)
        if self.config["ENGINE_VARIANT"] != "gpu":
            raise ClusterError("verify-gpu-exchange requires ENGINE_VARIANT=gpu")
        if self.workers < 2:
            raise ClusterError("verify-gpu-exchange requires at least two workers")
        _, workers = self.expected_inventory()
        peer_addresses = ",".join(
            item.private_ip or item.private_dns for item in workers
        )
        command_id = self.send_command(
            [worker.instance_id for worker in workers],
            [
                "set -eu",
                (
                    "bash /opt/velox-testing/presto/aws/ec2/remote/"
                    f"verify_gpu_exchange.sh {shlex.quote(peer_addresses)}"
                ),
            ],
            f"verify GPU exchange {self.run_id}",
            wait=True,
        )
        self.record_command("verify_gpu_exchange", command_id)

    def collect(self) -> None:
        self.validate(cloud=True)
        inventory = self.inventory()
        running = [item for item in inventory if item.state == "running"]
        if not running:
            raise ClusterError(f"RunId {self.run_id}: no running instances to collect")
        expected = self.workers + 1
        if len(running) != expected:
            print(
                f"warning: collecting {len(running)} of {expected} expected running instances",
                file=sys.stderr,
            )
        result_uri = self.result_uri()
        for item in running:
            commands = [
                "set -eu",
                (
                    "bash /opt/velox-testing/presto/aws/ec2/remote/collect_node.sh "
                    f"{shlex.quote(self.run_id)} {shlex.quote(item.role)} "
                    f"{shlex.quote(item.instance_id)} {shlex.quote(result_uri)}"
                ),
            ]
            command_id = self.send_command([item.instance_id], commands, f"collect {item.instance_id}")
            self.record_command(f"collect_{item.instance_id}", command_id)

    def stop(self) -> None:
        coordinator, workers = self.expected_inventory()
        command_id = self.send_command(
            [coordinator.instance_id, *[item.instance_id for item in workers]],
            [
                "set -eu",
                (
                    "docker rm -f presto-coordinator presto-native-worker-cpu "
                    "presto-native-worker-gpu 2>/dev/null || true"
                ),
            ],
            f"stop {self.run_id}",
        )
        self.record_command("stop", command_id)

    def terminate(self) -> None:
        instances = self.inventory()
        if not instances:
            print(f"RunId {self.run_id}: no live instances found")
            return
        ids = [item.instance_id for item in instances]
        self.aws.run(["ec2", "terminate-instances", "--instance-ids", *ids])
        if not self.dry_run:
            self.aws.run(["ec2", "wait", "instance-terminated", "--instance-ids", *ids])
        print(f"RunId {self.run_id}: terminated {len(ids)} instances")

    def list_stale(self) -> None:
        response = self.aws.run(
            [
                "ec2",
                "describe-instances",
                "--filters",
                f"Name=tag:Project,Values={PROJECT_TAG}",
                f"Name=tag:Experiment,Values={self.config['EXPERIMENT_TAG']}",
                "Name=instance-state-name,Values=pending,running,stopping,stopped",
            ],
            json_output=True,
            allow_dry_run=False,
        )
        now = utc_now()
        stale: list[dict[str, str]] = []
        for reservation in response.get("Reservations", []):
            for raw in reservation.get("Instances", []):
                tags = {tag["Key"]: tag["Value"] for tag in raw.get("Tags", [])}
                expiry_text = tags.get("ExpiresAt", "")
                if not expiry_text:
                    continue
                try:
                    expiry = dt.datetime.fromisoformat(expiry_text)
                except ValueError:
                    continue
                if expiry <= now:
                    stale.append(
                        {
                            "instance_id": raw["InstanceId"],
                            "run_id": tags.get("RunId", ""),
                            "role": tags.get("Role", ""),
                            "owner": tags.get("Owner", ""),
                            "expires_at": expiry_text,
                            "state": raw["State"]["Name"],
                        }
                    )
        print(json.dumps(sorted(stale, key=lambda item: item["expires_at"]), indent=2))

    def result_uri(self) -> str:
        require(self.config, "RESULTS_S3_ROOT")
        return f"{self.config['RESULTS_S3_ROOT'].rstrip('/')}/{self.run_id}"

    def write_manifest(self, payload: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.state_dir / "run_manifest.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    def record_command(self, name: str, command_id: str) -> None:
        if self.dry_run:
            return
        self.state_dir.mkdir(parents=True, exist_ok=True)
        path = self.state_dir / "ssm_commands.json"
        commands = json.loads(path.read_text()) if path.exists() else {}
        commands[name] = {"command_id": command_id, "recorded_at": utc_now().isoformat()}
        path.write_text(json.dumps(commands, indent=2, sort_keys=True) + "\n")


def default_run_id() -> str:
    return "presto-ec2-" + utc_now().strftime("%Y%m%dT%H%M%SZ").lower()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--run-id", default=default_run_id())
    parser.add_argument("--workers", type=int)
    parser.add_argument("--state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument("--dry-run", action="store_true")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in (
        "validate",
        "launch",
        "status",
        "bootstrap",
        "start",
        "verify-gpu-exchange",
        "collect",
        "stop",
        "terminate",
        "list-stale",
    ):
        subparsers.add_parser(name)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--queries", default="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22")
    run_parser.add_argument("--iterations", type=int, default=5)
    run_parser.add_argument("--tag", default="sf1k_q1q22")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        config = load_env(args.config)
        workers = args.workers or positive_int(config, "WORKER_COUNT")
        cluster = Cluster(
            config,
            run_id=args.run_id,
            workers=workers,
            state_root=args.state_root,
            dry_run=args.dry_run,
        )
        if args.command == "validate":
            cluster.validate(cloud=True)
            print("configuration is valid")
        elif args.command == "launch":
            cluster.launch()
        elif args.command == "status":
            print(json.dumps(cluster.inventory_json(), indent=2))
        elif args.command == "bootstrap":
            cluster.bootstrap()
        elif args.command == "start":
            cluster.start()
        elif args.command == "run":
            if args.iterations <= 0:
                raise ClusterError("--iterations must be greater than zero")
            cluster.run_benchmark(args.queries, args.iterations, args.tag)
        elif args.command == "verify-gpu-exchange":
            cluster.verify_gpu_exchange()
        elif args.command == "collect":
            cluster.collect()
        elif args.command == "stop":
            cluster.stop()
        elif args.command == "terminate":
            cluster.terminate()
        elif args.command == "list-stale":
            cluster.list_stale()
        return 0
    except (ClusterError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
