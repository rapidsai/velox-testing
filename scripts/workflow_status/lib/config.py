#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Configuration dataclass and loader for workflow_status."""

from __future__ import annotations

import os
from dataclasses import dataclass

# NVIDIA Inference Hub (Claude Code / Anthropic-compatible gateway).
# Claude Code appends /v1/messages — do not include /v1 on this URL.
# Override with --anthropic-base-url or ANTHROPIC_BASE_URL.
INFERENCE_HUB_BASE_URL = "https://inference-api.nvidia.com"


@dataclass
class Config:
    """All settings for a single workflow-status invocation.

    Populated from CLI args + environment variables by ``load_from_args``.
    Passed explicitly to every module — no global singleton.
    """

    run_id: int = 0
    job_id: int | None = None
    repo: str = ""
    output: str = ""
    slack: bool = False
    print_logs: bool = False
    analyze_cause: bool = True
    analyze_fix: bool = True

    log_tail_lines: int = 150
    gh_retries: int = 5
    gh_retry_sleep: int = 2
    gh_http_timeout: int = 60

    claude_bin: str = "claude"
    claude_model: str = "azure/anthropic/claude-opus-5"
    anthropic_base_url: str = INFERENCE_HUB_BASE_URL
    anthropic_auth_token: str = ""

    slack_webhook_url: str = ""

    max_gh_workers: int = 12
    max_ai_workers: int = 4


def load_from_args(args) -> Config:
    """Build a ``Config`` from an argparse namespace and environment variables."""
    cfg = Config(
        run_id=args.run_id,
        job_id=getattr(args, "job_id", None),
        repo=getattr(args, "repo", "") or "",
        output=getattr(args, "output", "") or "",
        slack=getattr(args, "slack", False),
        print_logs=getattr(args, "print_logs", False),
        analyze_cause=not getattr(args, "no_cause", False),
        analyze_fix=not getattr(args, "no_fix", False),
    )

    if cfg.analyze_cause is False:
        cfg.analyze_fix = False

    cfg.log_tail_lines = int(os.environ.get("LOG_TAIL_LINES", "150"))
    cfg.gh_retries = int(os.environ.get("GH_RETRIES", "5"))
    cfg.gh_retry_sleep = int(os.environ.get("GH_RETRY_SLEEP_SECONDS", "2"))
    cfg.gh_http_timeout = int(os.environ.get("GH_HTTP_TIMEOUT", "60"))

    cfg.claude_bin = getattr(args, "claude_bin", "") or os.environ.get("CLAUDE_BIN", "claude")
    cfg.claude_model = (
        getattr(args, "claude_model", "") or os.environ.get("CLAUDE_MODEL") or "azure/anthropic/claude-opus-5"
    )
    cfg.anthropic_base_url = (
        getattr(args, "anthropic_base_url", "") or os.environ.get("ANTHROPIC_BASE_URL") or INFERENCE_HUB_BASE_URL
    ).rstrip("/")
    cfg.anthropic_auth_token = (
        os.environ.get("ANTHROPIC_AUTH_TOKEN")
        or os.environ.get("NVIDIA_API_KEY")
        or os.environ.get("NGC_API_KEY")
        or ""
    )

    cfg.slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")

    os.environ["ANTHROPIC_BASE_URL"] = cfg.anthropic_base_url
    os.environ["CLAUDE_MODEL"] = cfg.claude_model
    if cfg.anthropic_auth_token:
        os.environ["ANTHROPIC_AUTH_TOKEN"] = cfg.anthropic_auth_token

    return cfg
