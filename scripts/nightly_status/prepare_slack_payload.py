#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Convert a Slack-formatted report file into a JSON payload file suitable for
slackapi/slack-github-action with payload-file-path + incoming webhook.

Sections are delimited by lines containing only '---'.
Each section becomes a separate mrkdwn block in the payload, with divider
blocks in between. Long sections are split to stay within Slack's 3000-char
block text limit.

Usage:
  python scripts/post_to_slack.py --file status.txt --output payload.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys

SLACK_BLOCK_TEXT_LIMIT = 3000


def split_into_sections(content: str) -> list[str]:
    """Split content on '---' lines into individual sections."""
    sections: list[str] = []
    current: list[str] = []

    for line in content.splitlines(keepends=True):
        if re.match(r"^\s*---\s*$", line):
            section_text = "".join(current).strip()
            if section_text:
                sections.append(section_text)
            current = []
        else:
            current.append(line)

    trailing = "".join(current).strip()
    if trailing:
        sections.append(trailing)

    return sections


def chunk_text(text: str, max_len: int = SLACK_BLOCK_TEXT_LIMIT) -> list[str]:
    """Split text into chunks that fit within Slack's block text limit."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    lines = text.splitlines(keepends=True)
    current: list[str] = []
    current_len = 0

    for line in lines:
        if current_len + len(line) > max_len and current:
            chunks.append("".join(current).rstrip())
            current = []
            current_len = 0
        current.append(line)
        current_len += len(line)

    if current:
        chunks.append("".join(current).rstrip())

    return chunks


def build_payload(sections: list[str]) -> dict:
    """Build a single Slack Block Kit payload from all sections."""
    blocks: list[dict] = []

    for i, section in enumerate(sections):
        if i > 0:
            blocks.append({"type": "divider"})

        for chunk in chunk_text(section):
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": chunk},
                }
            )

    fallback = sections[0][:200] if sections else ""
    return {"text": fallback, "blocks": blocks}


def main():
    parser = argparse.ArgumentParser(
        description="Convert report to Slack JSON payload for slackapi/slack-github-action"
    )
    parser.add_argument("--file", required=True, help="Path to the report file")
    parser.add_argument("--output", required=True, help="Path to write the JSON payload")
    args = parser.parse_args()

    try:
        with open(args.file) as f:
            content = f.read()
    except FileNotFoundError:
        print(f"ERROR: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    if not content.strip():
        print("Report file is empty, skipping.", file=sys.stderr)
        return

    sections = split_into_sections(content)
    if not sections:
        print("No sections found in report, skipping.", file=sys.stderr)
        return

    payload = build_payload(sections)

    with open(args.output, "w") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    block_count = len(payload["blocks"])
    print(f"Generated {args.output} ({block_count} blocks from {len(sections)} sections)", file=sys.stderr)


if __name__ == "__main__":
    main()
