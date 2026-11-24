# Velox Agent Skills

Skills for AI coding agents (Claude Code and Cursor) working on Velox.

| Skill                                    | What it does                                                         |
| ---------------------------------------- | -------------------------------------------------------------------- |
| [`velox-self-review`](velox-self-review) | Reviews your local Velox changes before you push or request review   |

## Install

```bash
velox/skills/install.sh
```

This links each skill into `~/.claude/skills` and `~/.cursor/skills`. Restart
Claude Code or Cursor afterwards.

Skills are loaded per user, not from whichever repository is open, which is why
they need installing: a skill sitting in velox-testing would never activate
while you work in a Velox checkout.

| Option        | Effect                                               |
| ------------- | ---------------------------------------------------- |
| `--claude`    | Claude Code only                                     |
| `--cursor`    | Cursor only                                          |
| `--copy`      | Copy instead of symlink                              |
| `--uninstall` | Remove what the script installed                     |

The script never overwrites or removes a same-named skill it did not install.

**Updating:** with the default symlinks, `git pull` in velox-testing updates the
skill everywhere. With `--copy`, re-run the installer after pulling.

## `velox-self-review`

Run it inside your Velox checkout:

```text
/velox-self-review                  everything not yet upstream on this branch
/velox-self-review --staged         only what you have staged
/velox-self-review --uncommitted    only uncommitted work
/velox-self-review velox/exec/      restrict to paths
/velox-self-review --deep           add a second pass for bugs and architecture
```

Or ask in plain words: *"review my changes before I push"*, *"deep review
HashBuild.cpp"*.

It compares your branch against upstream `main` — the remote whose URL is
`facebookincubator/velox`, whatever you named it — and reviews committed,
uncommitted, and new untracked files together. It reads whole files, base
classes, and sibling components rather than only the diff, and reports
findings by severity:

- 🔴 **Critical** — fix before you push: correctness, memory safety, lifetimes,
  concurrency, and violations of Velox's written rules
- 🟡 **Suggestion** — likely to come up in review: readability, tests,
  organization, performance
- 🟢 **Nitpick** — minor

Each finding cites a `file:line` and a concrete fix. At the end it offers to
apply the ones you pick.

**It is read-only until you ask for fixes.** It does not run `git fetch` or any
other git write, and it never commits or pushes. If your upstream ref is stale
enough that someone else's commits would be reviewed as yours, it tells you to
fetch rather than doing it.

The rules come from Velox's own `CODING_STYLE.md`, `CONTRIBUTING.md`, and
`.claude/CLAUDE.md`. The skill bundles a reference copy, but reads those files
from your checkout and follows them where they differ — so it tracks the
repository as the rules change.
