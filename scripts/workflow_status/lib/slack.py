#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Slack Block Kit payload builder, mrkdwn chunking, and webhook sender."""

from __future__ import annotations

import json
import re
import sys
import urllib.request

SLACK_BLOCK_TEXT_LIMIT = 3000


def split_mrkdwn_sections(content: str) -> list[str]:
    """Split mrkdwn content on '---' lines into individual sections."""
    sections: list[str] = []
    current: list[str] = []
    for line in content.splitlines(keepends=True):
        if re.match(r"^\s*---\s*$", line):
            text = "".join(current).strip()
            if text:
                sections.append(text)
            current = []
        else:
            current.append(line)
    trailing = "".join(current).strip()
    if trailing:
        sections.append(trailing)
    return sections


def split_code_blocks(section: str) -> list[str]:
    """Split a section into alternating text and code-fenced fragments."""
    fragments: list[str] = []
    current: list[str] = []
    in_code = False
    for line in section.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("```"):
            if in_code:
                current.append(line)
                fragments.append("".join(current).strip())
                current = []
                in_code = False
            else:
                text_before = "".join(current).strip()
                if text_before:
                    fragments.append(text_before)
                current = [line]
                in_code = True
        else:
            current.append(line)
    trailing = "".join(current).strip()
    if trailing:
        if in_code:
            trailing += "\n```"
        fragments.append(trailing)
    return [f for f in fragments if f]


def chunk_text(text: str, max_len: int = SLACK_BLOCK_TEXT_LIMIT) -> list[str]:
    """Split text into chunks that fit within Slack's block text limit."""
    if len(text) <= max_len:
        return [text]
    is_code = text.lstrip().startswith("```")
    chunks: list[str] = []
    lines = text.splitlines(keepends=True)
    current: list[str] = []
    current_len = 0
    for line in lines:
        if current_len + len(line) > max_len and current:
            chunk = "".join(current).rstrip()
            if is_code and not chunk.rstrip().endswith("```"):
                chunk += "\n```"
            chunks.append(chunk)
            current = []
            current_len = 0
            if is_code:
                current.append("```\n")
                current_len = 4
        current.append(line)
        current_len += len(line)
    if current:
        chunks.append("".join(current).rstrip())
    return chunks


def build_payload(mrkdwn_text: str) -> dict:
    """Convert mrkdwn text (with ``---`` separators) into a Slack Block Kit payload."""
    sections = split_mrkdwn_sections(mrkdwn_text)
    blocks: list[dict] = []
    for i, section in enumerate(sections):
        if i > 0:
            blocks.append({"type": "divider"})
        for fragment in split_code_blocks(section):
            for c in chunk_text(fragment):
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": c}})
    fallback = sections[0][:200] if sections else ""
    return {"text": fallback, "blocks": blocks}


def send_webhook(payload: dict, webhook_url: str) -> bool:
    """Post a Slack Block Kit payload to a webhook URL.

    Returns ``True`` on success, ``False`` on failure.
    """
    if not webhook_url:
        print("WARN: SLACK_WEBHOOK_URL not set, skipping Slack notification", file=sys.stderr)
        return False
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            ok = 200 <= resp.status < 300
            if ok:
                print("Slack notification sent successfully", file=sys.stderr)
            else:
                print(f"WARN: Slack webhook returned status {resp.status}", file=sys.stderr)
            return ok
    except Exception as exc:
        print(f"WARN: Slack webhook failed: {exc}", file=sys.stderr)
        return False
