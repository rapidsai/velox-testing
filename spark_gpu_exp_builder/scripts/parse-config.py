#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2025-2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Round-trip XML configuration toolkit for the Gluten GPU builder.

Uses only the Python3 standard library (xml.etree.ElementTree, json).

Subcommands:
    read <xml_file>             Parse XML config, emit shell KEY=VALUE lines.
    write <xml_file> [VAR=val]  Write config to XML from args, --env, or defaults.
    defaults                    Emit all default KEY=VALUE lines.
    shell-helpers               Emit CONFIG_DEF_TABLE + shell helper functions.

The config schema is read from config_def.json — the single source of truth.

Backward compatible: a bare filename with no subcommand defaults to 'read'.

Usage:
    eval "$(python3 parse-config.py read config.xml)"
    python3 parse-config.py write output.xml SPARK_VERSION=3.5 ENABLE_HDFS=ON
    python3 parse-config.py write output.xml --env
    python3 parse-config.py defaults
    eval "$(python3 parse-config.py shell-helpers)"
"""

import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ConfigEntry:
    shell_var: str
    xml_path: str   # "section/key" or "" (no XML persistence)
    default: str     # "" means no default
    required: bool
    env_alias: str   # "" means no alias


# ── JSON config loading ─────────────────────────────────────────────────────

def _load_full_config() -> dict:
    """Load the full config_def.json."""
    path = Path(__file__).parent / "config_def.json"
    return json.loads(path.read_text())


def _load_config_defs() -> list[ConfigEntry]:
    """Load config definitions from config_def.json as a flat ConfigEntry list."""
    data = _load_full_config()
    entries: list[ConfigEntry] = []
    for section, keys in data["entries"].items():
        for key, e in keys.items():
            xml_path = "" if section == "_internal" else f"{section}/{key}"
            entries.append(ConfigEntry(
                shell_var=e["shell_var"],
                xml_path=xml_path,
                default=e["default"],
                required=e["required"],
                env_alias=e["env_alias"],
            ))
    return entries


# ── Subcommand: read ─────────────────────────────────────────────────────────

def cmd_read(xml_path: str, entries: list[ConfigEntry]) -> None:
    """Parse XML config and emit shell KEY=VALUE lines.

    When a config file is used, it is the sole config source. Values are always
    emitted regardless of what is in the environment. ${VAR} references in
    values are expanded from the environment at read time.
    CLI flags (parsed after eval) still take precedence.
    """
    if not os.path.isfile(xml_path):
        print(f"Config file not found: {xml_path}", file=sys.stderr)
        sys.exit(1)

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Build reverse map: xml_path -> ConfigEntry
    path_to_entry: dict[str, ConfigEntry] = {}
    for entry in entries:
        if entry.xml_path:
            path_to_entry[entry.xml_path] = entry

    for section in root:
        section_name = section.tag

        # Leaf element directly under root (no children).
        if len(section) == 0:
            value = (section.text or "").strip()
            xml_key = section_name
            _emit_read(xml_key, value, path_to_entry)
            continue

        # Section with child elements.
        for child in section:
            # Collect all text: element text + tail of each sub-child (covers
            # elements that contain comment nodes, where the real value sits in
            # the tail of the last comment child rather than in child.text).
            parts = [child.text or ""]
            for sub in child:
                parts.append(sub.tail or "")
            value = "".join(parts).strip()
            xml_key = f"{section_name}/{child.tag}"
            _emit_read(xml_key, value, path_to_entry)


def _expand_env_refs(value: str) -> tuple[str, list[str]]:
    """Expand ${VAR} references in value from the current environment.

    Returns (expanded_value, list_of_unresolved_var_names).
    """
    unresolved: list[str] = []

    def _replace(m: re.Match) -> str:
        var = m.group(1)
        val = os.environ.get(var, "")
        if not val:
            unresolved.append(var)
        return val

    expanded = re.sub(r"\$\{(\w+)\}", _replace, value)
    return expanded, unresolved


def _emit_read(
    xml_path: str,
    value: str,
    path_to_entry: dict[str, ConfigEntry],
) -> None:
    """Emit KEY=VALUE for a single XML element.

    Config file is the sole source — always emit, no env-skip logic.
    """
    if not value:
        return

    # Expand ${VAR} references from environment.
    value, unresolved = _expand_env_refs(value)
    if unresolved:
        entry = path_to_entry.get(xml_path)
        var_name = entry.shell_var if entry else xml_path
        for u in unresolved:
            print(
                f"WARNING: {var_name}: ${{{u}}} is not set in environment",
                file=sys.stderr,
            )
    if not value:
        return

    entry = path_to_entry.get(xml_path)
    if entry is None:
        # Unknown XML element — emit as SECTION_KEY (uppercase) for compat.
        key = xml_path.replace("/", "_").upper()
        print(f'{key}="{value}"')
        return

    print(f'{entry.shell_var}="{value}"')


# ── Subcommand: write ────────────────────────────────────────────────────────

def _expand_elements_with_descriptions(xml_str: str, path_descriptions: dict[str, tuple[bool, str]]) -> str:
    """Add description comments to empty and env-ref elements.

    Self-closing:
      <key />  →  <key>
                    <!-- optional: desc -->
                  </key>

    Inline with env-ref value:
      <key>${VAR}</key>  →  <key>
                              <!-- required: desc -->
                              ${VAR}
                            </key>
    """
    lines = xml_str.split("\n")
    result = []
    current_section: str = ""
    for line in lines:
        # Track current section open tag (e.g. <docker>).
        m_section = re.match(r"^(\s*)<(\w+)>\s*$", line)
        if m_section:
            current_section = m_section.group(2)

        # Match self-closing empty element: "  <key />"
        m_empty = re.match(r"^(\s*)<(\w+)\s*/>\s*$", line)
        if m_empty:
            indent = m_empty.group(1)
            key = m_empty.group(2)
            xml_path = f"{current_section}/{key}" if current_section else key
            if xml_path in path_descriptions:
                req, desc = path_descriptions[xml_path]
                req_str = "required" if req else "optional"
                result.append(f"{indent}<{key}>")
                result.append(f"{indent}  <!-- {req_str}: {desc} -->")
                result.append(f"{indent}</{key}>")
                continue

        # Match inline element with value: "  <key>value</key>"
        m_inline = re.match(r"^(\s*)<(\w+)>(.+)</\2>\s*$", line)
        if m_inline:
            indent = m_inline.group(1)
            key = m_inline.group(2)
            val = m_inline.group(3)
            xml_path = f"{current_section}/{key}" if current_section else key
            if xml_path in path_descriptions:
                req, desc = path_descriptions[xml_path]
                req_str = "required" if req else "optional"
                result.append(f"{indent}<{key}>")
                result.append(f"{indent}  <!-- {req_str}: {desc} -->")
                result.append(f"{indent}  {val}")
                result.append(f"{indent}</{key}>")
                continue

        result.append(line)
    return "\n".join(result)


def cmd_write(
    output_path: str,
    entries: list[ConfigEntry],
    var_overrides: dict[str, str],
    from_env: bool,
) -> None:
    """Generate XML config from variable values.

    Value resolution order for each entry:
        1. var_overrides (explicit VAR=val args)
        2. os.environ[shell_var] or os.environ[env_alias] (if --env)
        3. env_ref_default from JSON (e.g. "${GLUTEN_DIR}")
        4. entry.default
    """
    data = _load_full_config()
    section_order: list[str] = data["section_order"]

    # Build lookup dicts from JSON for descriptions and env-ref defaults.
    entry_descriptions: dict[str, tuple[bool, str]] = {}
    env_ref_defaults: dict[str, str] = {}
    for section, keys in data["entries"].items():
        if section == "_internal":
            continue
        for key, e in keys.items():
            if e["description"]:
                entry_descriptions[e["shell_var"]] = (e["required"], e["description"])
            if e["env_ref_default"]:
                env_ref_defaults[e["shell_var"]] = e["env_ref_default"]

    root = ET.Element("builder")
    sections: dict[str, ET.Element] = {}

    for entry in entries:
        if not entry.xml_path:
            continue
        section_name, key = entry.xml_path.split("/")

        # Resolve value.
        value = ""
        if entry.shell_var in var_overrides:
            value = var_overrides[entry.shell_var]
        elif from_env:
            value = os.environ.get(entry.shell_var, "")
            if not value and entry.env_alias:
                value = os.environ.get(entry.env_alias, "")
        if not value and entry.shell_var in env_ref_defaults:
            value = env_ref_defaults[entry.shell_var]
        elif not value:
            value = entry.default

        # Create section if needed.
        if section_name not in sections:
            sections[section_name] = ET.SubElement(root, section_name)

        child = ET.SubElement(sections[section_name], key)
        child.text = value

    # Order sections.
    ordered: list[ET.Element] = []
    for name in section_order:
        if name in sections:
            ordered.append(sections[name])
    for name, elem in sections.items():
        if name not in section_order:
            ordered.append(elem)
    root[:] = ordered

    ET.indent(root, space="  ")

    # Build xml_path -> (required, description) for empty-element expansion.
    path_descriptions: dict[str, tuple[bool, str]] = {}
    for entry in entries:
        if entry.xml_path and entry.shell_var in entry_descriptions:
            path_descriptions[entry.xml_path] = entry_descriptions[entry.shell_var]

    # Build output string with comment header.
    lines = [
        "<!--",
        "  Builder configuration file.",
        "  Generated by: parse-config.py write",
        "",
        "  When config file is used, this file is the sole config source.",
        "  Environment variables are NOT read — use ${VAR} syntax in values",
        "  to reference them (expanded at read time).",
        "",
        "  CLI flags still take precedence over config file values.",
        "",
        "  Empty elements are ignored and fall back to script defaults.",
        "-->",
    ]
    xml_str = ET.tostring(root, encoding="unicode")
    xml_str = _expand_elements_with_descriptions(xml_str, path_descriptions)
    lines.append(xml_str)
    output = "\n".join(lines) + "\n"

    if output_path == "-":
        sys.stdout.write(output)
    else:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(output)


# ── Subcommand: defaults ─────────────────────────────────────────────────────

def cmd_defaults(entries: list[ConfigEntry]) -> None:
    """Emit all defaults as KEY=VALUE lines."""
    for entry in entries:
        if entry.default:
            print(f'{entry.shell_var}="{entry.default}"')


# ── Subcommand: shell-helpers ────────────────────────────────────────────────

_SHELL_FUNCTIONS = r'''
# Read env aliases into their canonical shell variables.
apply_env_aliases() {
  local var_name xml_path default required env_alias
  while IFS= read -r _line; do
    [[ -z "$_line" || "$_line" =~ ^[[:space:]]*# ]] && continue
    read -r var_name xml_path default required env_alias <<< "$_line"
    [[ "$env_alias" == "-" ]] && continue
    [[ -n "${!var_name:-}" ]] && continue
    if [[ -n "${!env_alias:-}" ]]; then
      eval "$var_name=\"${!env_alias}\""
      eval "_${var_name}_FROM_ENV=env"
    fi
  done <<< "$CONFIG_DEF_TABLE"
}

# Apply defaults for any config variable still empty.
apply_defaults() {
  local var_name xml_path default required env_alias
  while IFS= read -r _line; do
    [[ -z "$_line" || "$_line" =~ ^[[:space:]]*# ]] && continue
    read -r var_name xml_path default required env_alias <<< "$_line"
    [[ "$default" == "-" ]] && continue
    if [[ -z "${!var_name:-}" ]]; then
      eval "$var_name=\"$default\""
    fi
  done <<< "$CONFIG_DEF_TABLE"
}

# Validate that all required config variables are set.
validate_required() {
  local var_name xml_path default required env_alias
  local missing=()
  while IFS= read -r _line; do
    [[ -z "$_line" || "$_line" =~ ^[[:space:]]*# ]] && continue
    read -r var_name xml_path default required env_alias <<< "$_line"
    [[ "$required" != "yes" ]] && continue
    if [[ -z "${!var_name:-}" ]]; then
      missing+=("$var_name")
    fi
  done <<< "$CONFIG_DEF_TABLE"
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: Required config variable(s) not set: ${missing[*]}" >&2
    return 1
  fi
}
'''


def cmd_shell_helpers() -> None:
    """Emit CONFIG_DEF_TABLE variable and shell helper functions.

    Output is eval-safe shell code that defines:
      - CONFIG_DEF_TABLE (space-separated table, same format as the old heredoc)
      - apply_env_aliases()
      - apply_defaults()
      - validate_required()
    """
    data = _load_full_config()

    # Build the table lines from JSON entries.
    table_lines: list[str] = []
    for section, keys in data["entries"].items():
        for key, e in keys.items():
            xml_path = "-" if section == "_internal" else f"{section}/{key}"
            default = e["default"] or "-"
            required = "yes" if e["required"] else "no"
            env_alias = e["env_alias"] or "-"
            table_lines.append(f'{e["shell_var"]} {xml_path} {default} {required} {env_alias}')

    table = "\n".join(table_lines)
    print(f"CONFIG_DEF_TABLE='{table}'")
    print(_SHELL_FUNCTIONS)


# ── Main ─────────────────────────────────────────────────────────────────────

def _usage():
    print(
        "Usage:\n"
        "  parse-config.py read <xml_file>              Read XML → shell vars\n"
        "  parse-config.py write <xml_file> [VAR=val..] Write shell vars → XML\n"
        "  parse-config.py write <xml_file> --env        Write from environment\n"
        "  parse-config.py defaults                      Emit all defaults\n"
        "  parse-config.py shell-helpers                 Emit shell CONFIG_DEF_TABLE + helpers\n",
        file=sys.stderr,
    )


def main():
    args = sys.argv[1:]
    if not args:
        _usage()
        sys.exit(1)

    subcmd = args[0]

    # shell-helpers doesn't need entries loaded via _load_config_defs()
    # (it reads JSON directly), so handle it before the general entry loading.
    if subcmd == "shell-helpers":
        cmd_shell_helpers()
        return

    entries = _load_config_defs()

    # Backward compat: if first arg looks like a file path (not a subcommand),
    # treat it as `read <file>`.
    if subcmd not in ("read", "write", "defaults", "-h", "--help"):
        # Assume it's a file path — backward-compatible `read` mode.
        cmd_read(subcmd, entries)
        return

    if subcmd in ("-h", "--help"):
        _usage()
        sys.exit(0)

    if subcmd == "read":
        if len(args) < 2:
            print("ERROR: read requires <xml_file>", file=sys.stderr)
            sys.exit(1)
        cmd_read(args[1], entries)

    elif subcmd == "write":
        if len(args) < 2:
            print("ERROR: write requires <xml_file>", file=sys.stderr)
            sys.exit(1)
        output_path = args[1]
        from_env = "--env" in args
        var_overrides: dict[str, str] = {}
        for arg in args[2:]:
            if arg == "--env":
                continue
            if "=" in arg:
                k, v = arg.split("=", 1)
                var_overrides[k] = v
            else:
                print(f"WARNING: ignoring unknown arg: {arg}", file=sys.stderr)
        cmd_write(output_path, entries, var_overrides, from_env)

    elif subcmd == "defaults":
        cmd_defaults(entries)


if __name__ == "__main__":
    main()
