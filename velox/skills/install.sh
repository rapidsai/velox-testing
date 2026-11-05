#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Install the agent skills in this directory for Claude Code and Cursor.
#
# Skills are loaded per user (~/.claude/skills, ~/.cursor/skills), not from the
# repository you happen to be in, so a skill kept here would never activate in
# a Velox checkout on its own. This links each skill into those directories.
#
# The default is a symlink, so `git pull` in velox-testing updates the skill
# everywhere with no reinstall. Use --copy where symlinks are not wanted.
#
# Usage:
#   install.sh                 link every skill into Claude Code and Cursor
#   install.sh --claude        Claude Code only
#   install.sh --cursor        Cursor only
#   install.sh --copy          copy instead of symlink
#   install.sh --uninstall     remove what this script installed
#
# An existing skill of the same name that this script did not install is never
# overwritten or removed.

set -euo pipefail

SKILLS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MARKER=".installed-by-velox-testing"

targets=()
mode="link"
action="install"

for arg in "$@"; do
  case "$arg" in
    --claude) targets+=("$HOME/.claude/skills") ;;
    --cursor) targets+=("$HOME/.cursor/skills") ;;
    --copy) mode="copy" ;;
    --uninstall) action="uninstall" ;;
    -h | --help)
      sed -n '/^# Install the agent skills/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown option: $arg (see --help)" >&2
      exit 2
      ;;
  esac
done

if [ ${#targets[@]} -eq 0 ]; then
  targets=("$HOME/.claude/skills" "$HOME/.cursor/skills")
fi

# A skill is any subdirectory holding a SKILL.md.
skills=()
for dir in "$SKILLS_DIR"/*/; do
  [ -f "$dir/SKILL.md" ] && skills+=("$(basename "$dir")")
done
if [ ${#skills[@]} -eq 0 ]; then
  echo "no skills found in $SKILLS_DIR" >&2
  exit 1
fi

# Whether a destination was put there by this script, and so is ours to
# replace or remove: a symlink into this directory, or a copy carrying the
# marker file.
is_ours() {
  local dest=$1 source=$2
  if [ -L "$dest" ]; then
    [ "$(readlink -f "$dest")" = "$(readlink -f "$source")" ]
  else
    [ -f "$dest/$MARKER" ]
  fi
}

install_skill() {
  local source=$1 dest=$2
  if [ -e "$dest" ] || [ -L "$dest" ]; then
    if ! is_ours "$dest" "$source"; then
      echo "  skip     $dest (exists and was not installed by this script)"
      return
    fi
    rm -rf "$dest"
  fi
  if [ "$mode" = copy ]; then
    cp -R "$source" "$dest"
    touch "$dest/$MARKER"
    echo "  copied   $dest"
  else
    ln -s "$source" "$dest"
    echo "  linked   $dest -> $source"
  fi
}

uninstall_skill() {
  local source=$1 dest=$2
  if ! [ -e "$dest" ] && ! [ -L "$dest" ]; then
    return
  fi
  if is_ours "$dest" "$source"; then
    rm -rf "$dest"
    echo "  removed  $dest"
  else
    echo "  skip     $dest (not installed by this script)"
  fi
}

for target in "${targets[@]}"; do
  echo "$target"
  [ "$action" = install ] && mkdir -p "$target"
  for skill in "${skills[@]}"; do
    "${action}_skill" "$SKILLS_DIR/$skill" "$target/$skill"
  done
done

if [ "$action" = install ]; then
  echo
  echo "Restart Claude Code or Cursor, then in a Velox checkout run:"
  echo "  /velox-self-review"
fi
