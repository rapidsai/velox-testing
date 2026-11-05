---
name: velox-self-review
description: Review your own local Velox changes before you push or request review — the current branch against upstream main, only staged changes, or specific files — for memory safety, lifetimes, correctness, concurrency, and Velox idiom violations that CI cannot catch. Use when asked to self-review, pre-review, or check local or uncommitted changes in a Velox checkout. Add "deep review" or --deep for a second, bugs-and-architecture-focused pass.
---

# Velox Self-Review

Review the changes in a local Velox checkout **before** they are pushed, so the
author can fix them first. Focus on what CI cannot catch: memory safety,
lifetimes, correctness, concurrency, and Velox idiom violations.

This reviews **local code**. It does not fetch PRs, post comments, or write to
GitHub. To review someone else's PR, check out their branch and run this on it.

## Usage

| Invocation                              | Reviews                                                                 |
| --------------------------------------- | ----------------------------------------------------------------------- |
| `/velox-self-review`                    | Everything not yet upstream: branch commits + uncommitted + new files   |
| `/velox-self-review --staged`           | Only what is staged (`git add`ed)                                       |
| `/velox-self-review --uncommitted`      | Only uncommitted work, ignoring commits already on the branch           |
| `/velox-self-review <path>...`          | Any of the above, restricted to these files or directories              |
| `/velox-self-review --base <ref>`       | Compare against `<ref>` instead of the discovered upstream `main`       |
| `/velox-self-review --deep`             | Adds the second-opinion pass (also triggered by "deep review")          |

Natural-language requests map onto these: "review my changes", "check what I
have staged", "deep review velox/exec/HashBuild.cpp".

## Ground Rules

- **The review is read-only.** Do not edit files, and do not run any git
  command that writes: no `fetch`, `checkout`, `switch`, `stash`, `reset`,
  `commit`, `add`, `rebase`. The author's working tree is their work in progress.
- **Never commit or push**, even after fixes are applied.
- Fixes happen only after the review, only for the findings the author picks,
  and only when asked (see [After the Review](#after-the-review)).

---

## Step 1: Read the Standards

Read both reference files bundled with this skill. They are in the
`references/` directory next to this `SKILL.md`:

```
references/velox-review-kb.md          # full rules, sources, change-type checklists
references/velox-review-checklist.md   # condensed form, red flags, output format
```

Then read the repository's own rules **from the checkout under review**, when
present:

```
CODING_STYLE.md
.claude/CLAUDE.md
```

The bundled references are a snapshot; the checkout is the rule. When they
disagree, follow the checkout and cite it. Consult the other docs listed in the
KB's Source Map (`CONTRIBUTING.md`, `website/docs/community/*.md`) when a
finding depends on them.

## Step 2: Find What Changed

### 2a. Confirm this is a Velox checkout

```bash
ROOT=$(git rev-parse --show-toplevel) || { echo "not a git repository"; exit 1; }
[ -d "$ROOT/velox" ] && [ -f "$ROOT/CODING_STYLE.md" ] || echo "not a Velox checkout"
```

If it is not a Velox checkout, say so and stop. Identify Velox by its files,
not its remote: forks and mirrors are named anything.

### 2b. Find the base to compare against

Compare against upstream `main` — the `facebookincubator/velox` remote. Find it
by **URL**, never by remote name: a checkout may call it `upstream`, `origin`,
`fb`, or anything else. Honour `--base <ref>` when given.

```bash
BASE_REF=""
for remote in $(git remote); do
  case "$(git remote get-url "$remote")" in
    *facebookincubator/velox*)
      if git rev-parse --verify -q "$remote/main" >/dev/null; then
        BASE_REF="$remote/main"
        break
      fi ;;
  esac
done
for fallback in origin/main main; do
  [ -n "$BASE_REF" ] && break
  git rev-parse --verify -q "$fallback" >/dev/null && BASE_REF="$fallback"
done
MERGE_BASE=$(git merge-base HEAD "$BASE_REF")
```

Do **not** `git fetch` to refresh the base — that writes to the repository. A
stale remote-tracking ref is fine as long as it is an ancestor of the branch;
2d checks for the case where it is not.

### 2c. Collect the change set

| Mode            | Tracked changes                    | New untracked files                   |
| --------------- | ---------------------------------- | ------------------------------------- |
| default         | `git diff "$MERGE_BASE"`           | `new_source_files` (below)            |
| `--uncommitted` | `git diff HEAD`                    | `new_source_files` (below)            |
| `--staged`      | `git diff --cached`                | none — untracked files are not staged |

`git diff "$MERGE_BASE"` compares the **working tree** to the merge base, so it
covers commits on the branch and uncommitted edits in one diff. On `main` with
no local commits, the merge base is `HEAD` and the default mode naturally
reduces to "uncommitted only".

**New source files are part of the change.** A new `.cpp` that has not been
`git add`ed yet is exactly what a self-review must not miss, and `git diff`
never shows it. But do **not** take every untracked file: a typical checkout
has thousands — clangd's index alone puts one `.idx` per source file under
`.cache/` — plus logs, benchmark output, and personal notes. Keep only source
files outside hidden directories, and function docs, which the KB requires for
new functions:

```bash
new_source_files() {
  git ls-files --others --exclude-standard \
    | grep -vE '(^|/)\.' \
    | grep -E '\.(cpp|cc|h|hpp|cu|cuh|py|cmake)$|(^|/)CMakeLists\.txt$|^velox/docs/.*\.rst$'
}
```

If the filter drops files that look like they belong to the change — a new
`.json` test fixture next to a new test, say — ask the author rather than
guessing either way.

Append `-- <path>...` to each command when paths were given.

If the change set is empty, say there is nothing to review and stop.

### 2d. Sanity-check the base

```bash
git log --format='%h %ae %s' "$MERGE_BASE"..HEAD
```

If this lists commits by authors other than the person running the review, the
base ref is probably stale: the branch contains upstream commits the local
`$BASE_REF` does not know about, and they would be reviewed as if they were the
author's. Stop and tell the author to run `git fetch <remote>`, or to pass
`--base`. Do not run the fetch yourself.

The commit subjects here are also the closest thing to a PR title and
description — use them in Step 3.

### 2e. Build the changed-files manifest

| Category           | Action                                                                          |
| ------------------ | ------------------------------------------------------------------------------- |
| **New files**      | Read the full file (mandatory) — includes untracked files                       |
| **Deleted files**  | Search for remaining references to the symbols the file defined                 |
| **Modified files** | Read the full file, not just changed hunks. Step 5a defines read depth          |

For deleted headers, verify cleanup is complete:

```bash
rg 'DeletedFileName' --type cpp
```

Note the size. If the change is well past ~400 changed lines, say so in the
summary — reviewers will likely ask for it to be split. [Source: `CONTRIBUTING.md`]

## Step 3: Classify the Change

Determine the change type from the commit subjects and the diff, and apply the
matching checklist from the KB (Section 7):

| Change Type  | Key Checks                                                                                        |
| ------------ | ------------------------------------------------------------------------------------------------- |
| Bug fix      | Failing-first test? Root cause? Edge cases? Correct CHECK family? Causation verified empirically? |
| Performance  | Benchmark data? Before/after? Data-driven? Proportionate complexity?                              |
| New API      | Minimal surface? Header docs? Impls in `.cpp`? No friend? No default args? No leaked internals?    |
| Refactor     | Separate from logic? Existing tests unchanged? No format churn mixed in?                          |
| New function | Upstream semantics? Fuzzer? `.rst` docs? Edge cases (null, max, type boundaries)?                 |
| Tests-only   | Correct semantics? gtest matchers? Grouped by topic? Error paths?                                 |
| Docs-only    | Accurate? Code examples tested? Performance claims backed?                                        |

If the change mixes types — a feature plus an unrelated fix — that is itself a
finding: reviewers will ask for separate PRs.

## Step 4: Two-Pass Review

**Pass 1 — Severity (catch the dangerous):**

1. **Correctness** — logic errors, off-by-one, null/empty/boundary, overflow,
   NaN/Inf, missing arguments, floating-point edge cases
2. **Memory safety** — use-after-free, dangling refs, leaks, ownership,
   scope/lifetime issues, stream synchronization (GPU code), shadowed members
   hiding leaks, exception safety
3. **Concurrency** — races, deadlocks, lock ordering, shared state
4. **Error handling** — correct CHECK/USER_CHECK family, comparison macros
   (`VELOX_CHECK_LT` not `VELOX_CHECK(a < b)`), runtime data at end of messages,
   `VELOX_FAIL`/`VELOX_UNREACHABLE`/`VELOX_NYI` usage
5. **Performance** — unnecessary copies, hot-path allocations, missing move
   semantics, cache-unfriendly access

**Pass 2 — Common feedback (what reviewers raise most, in order):**

6. **Readability** — simplify logic, suggest clearer alternatives
7. **Testing quality** — missing tests, edge cases, test readability, gtest
   matchers, `/*argName=*/value` format, no `TestValue`
8. **Code organization** — separate PRs, move logic to `.cpp`, extract shared
   code, reduce duplication
9. **Naming & style** — full descriptive names (not `val`/`idx`/`agg`),
   `numXxx` not `xxxCount`, no `*Utils`/`*Helpers`/`*Common`, remove unused
   headers/code
10. **Documentation** — `///` public API only, `//` everything else, active
    verbs, full sentences, no restating the variable name

## Step 5: Read Surrounding Code

This is where reviews fail most often: the diff alone hides the context that
makes a change wrong. The full checkout is right here, with full history — use
it.

### 5a. Read full files, not just diffs

This step is the single source of truth for read depth.

Read the **entire file** for every new file and every modified file. Full reads
surface shadowed members, lifetime bugs, and inconsistent patterns, so this is
the default, not an aspiration.

Only when a file exceeds **1500 lines** may the read be reduced, and it must
still cover:
- The class/struct declaration (all members, inheritance)
- The constructor and destructor
- Every method that appears in the diff
- 20-30 lines of context above and below each changed hunk

**Track coverage per region, never per file.** After a reduced read, record what
was covered — `full file`, or `lines 1-140 plus addInput(), getOutput()` — and
treat everything outside it as unread. **Re-read when in doubt**, and always
re-read the exact region before citing a line number.

**Batch the manifest reads.** The 2e manifest is known before this step starts,
so issue those reads in one parallel batch. The reads in 5b and 5c are
*discovered* from what 5a reveals, so they are later, dependent waves — do not
fold them into the first batch.

History is available, so `git log -p` and `git blame` on a changed region are
worth using to recover why a line looked the way it did before the change.

### 5b. Base class verification (mandatory for new classes)

For each new class, read its base class header **in full** — exempt from the
1500-line threshold and from any coverage-based skipping. This is the step that
catches member shadowing and virtual-override mismatches.

1. List all virtual methods — verify the subclass overrides the right ones.
2. List all member variables — check whether a new member shadows a base class
   member (which hides cleanup in `close()` or the destructor).
3. Check `close()`/destructor patterns — does the base class expect subclasses
   to release resources in a specific order?

### 5c. Sibling pattern comparison

Read at least one similar component in the same directory and check
consistency. For operators: how do siblings handle `getOutput()` return
semantics, the `isFinished()` state machine, stream synchronization? For
functions: how do nearby functions handle error paths, naming, signatures?

### 5d. Call-site signature verification

For every non-trivial function **called** in the change (especially GPU code),
verify the call matches the declaration. Locate identifiers in one pass rather
than one search per function, capping each pass at about 20-30 identifiers:

```bash
rg -n --type cpp -e 'functionOne\(' -e 'functionTwo\(' -e 'functionThree\('
```

**Batch to discover, confirm to cite.** A hit is a lead, not a verified
signature: it matches definitions, declarations, and call sites alike. Read the
specific declaration and confirm its argument list before writing any finding
that cites it.

Pay special attention to `mr` (memory resource) and `stream` arguments in
GPU/cuDF code — omitting them is a common and dangerous mistake.

### 5e. Test utility verification

Check that Velox-specific test helpers are used where they apply:
- `VELOX_ASSERT_THROW` instead of raw `EXPECT_THROW`
- `VELOX_CHECK_EQ` family instead of `VELOX_CHECK(a == b)`
- gtest matchers (`ElementsAre`, `UnorderedElementsAre`, `IsEmpty`, `SizeIs`)

## Step 6: Cross-Check Red Flags and Evidence

After both passes, check every item in the checklist's Blocking Red Flags table
explicitly. The ones most often missed:

- Missing `mr` / `stream` arguments in GPU code
- Scope/lifetime issues with temporaries, especially before stream sync
- A member variable shadowing a base class member (hides cleanup in `close()`)
- Missing Velox test macros (`VELOX_ASSERT_THROW`, `VELOX_CHECK_EQ`)
- Inheritance pattern inconsistent with sibling operators
- `getOutput()` returning a member without `std::move()` (breaks `isFinished()`)
- `#define` macros where inline functions or templates work

Then check the KB's Evidence Expectations (Section 8): does the change have the
evidence its type requires — a failing-first test for a bug fix, benchmark data
for a performance claim? Missing evidence is a finding, because the PR will be
asked for it.

---

## Output

Assign every finding a stable ID (`F1`, `F2`, …), sequential across severities.
Write findings for the author — direct, specific, fixable.

```text
### Summary
<1-2 sentences: what the change does, and whether it is ready to push.>
<Note if the change is large, mixes unrelated concerns, or lacks required evidence.>

### Critical Issues (🔴)
- **F1** [file:line] Issue. Risk: why it matters. Fix: concrete change.

### Suggestions (🟡)
- **F2** [file:line] Issue. Why it matters. Concrete alternative.

### Nitpicks (🟢)
- **F3** [file:line] Issue.

### What's Good
<1-2 sentences on a specific pattern worth keeping. Skip if nothing stands out.>
```

**Rules:**
- Cite exact `file:line` — never "somewhere in the code"
- Be actionable: a specific fix, with a code snippet where it helps
- Skip what CI catches: formatting, include order, type errors
- Each issue once
- Assume competence; explain only Velox-specific context
- When the docs are silent, frame it as a question, not a mandate
- Proportionate: 3 critical issues beat 20 nitpicks
- No filler ("it's worth noting", "presumably") — state the fact or ask

If there are no findings, say so plainly. Do not invent nitpicks to fill the
template.

## After the Review

End by offering to apply fixes, e.g. *"Want me to fix F1 and F3?"* Then:

- Apply only the findings the author chooses.
- Re-read each region before editing it; the author may have changed it since.
- After applying, summarize what changed per finding ID.
- **Never commit or push.** The author reviews the edits and commits them.

---

# Deep Review (Second Opinion)

Opt-in. Triggered by `--deep`, "deep review", "second opinion", or "review for
bugs/logic/architecture". Runs **after** the standard review, as a second pass
focused only on what that review is likely to have missed.

## Focus

- Logic bugs, hidden edge cases, and invariant violations
- Memory safety, ownership, lifecycle, and state-machine issues
- Concurrency hazards, shared-state assumptions, and exception safety
- Architecture: layering, cohesion, API boundaries, hidden coupling
- Missing tests only where they hide bug risk

**Skip:** naming, comment wording, formatting, and minor style — unless they
hide a correctness or architecture problem.

## How to Run It

**If your environment can launch a subagent** (Claude Code's Agent/Task tool,
Cursor's background or sub-agents), launch a **read-only** one with the current
model, and give it:

1. The change set and manifest from Step 2
2. Both reference files
3. The standard review's findings, so it can skip what is already covered

```text
Second-opinion review of these local Velox changes. The standard review's
findings are below. Find what they MISSED:
1. correctness and hidden logic bugs
2. lifetime, ownership, and state-machine invariants
3. concurrency, exception safety, and regression risk
4. architecture, boundaries, and hidden coupling

Read surrounding code, not just the diff. Do not edit any file.
Skip style and naming unless they hide a bug.
Return only NEW findings. Use 🔴/🟡; omit 🟢 unless it affects correctness.

--- Standard review findings ---
<findings>
```

**If it cannot**, run the second pass yourself after the standard review:
re-read the riskiest regions with only the focus above in mind, and report only
new findings.

## Merging

- Deduplicate identical issues; keep the higher severity
- Continue the ID sequence (`F7`, `F8`, …) — never renumber existing findings
- Mark second-pass findings with `(deep)` so the author can see their source

---

# Common Agent Failures

Check yourself against each:

| Failure                                                       | Do instead                                                                       |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Reviewing only `git diff`, missing untracked new files        | Add `new_source_files` (Step 2c)                                                 |
| Reviewing every untracked file                                | Filter to source files — `.cache/` alone can hold thousands (Step 2c)            |
| Reviewing upstream commits as if they were the author's       | Check commit authors against the merge base; a stale base ref causes this (2d)   |
| Finding the base by remote name (`upstream`, `origin`)        | Match the `facebookincubator/velox` URL (Step 2b)                                |
| Running `git fetch` or any other git write                    | The review is read-only; tell the author what to run (Ground Rules)              |
| Editing files during the review                               | Review first; fix only what the author picks, when asked (After the Review)      |
| Committing or pushing after applying fixes                    | Never — the author commits                                                       |
| Reading only diff hunks, not full files                       | Read the complete file for every new and modified file (5a)                      |
| Reading manifest files one at a time                          | Batch all 2e manifest reads in parallel (5a)                                     |
| Folding 5b/5c reads into the 5a batch                         | They are discovered from 5a — later, dependent waves                             |
| Marking a file "read" after a reduced read                    | Coverage is per region; record the ranges actually read (5a)                     |
| Skipping a base class header because 5a touched the file      | 5b always reads the base class header in full                                    |
| Citing a signature straight from a search hit                 | Read the declaration and confirm the argument list first (5d)                    |
| Missing `mr` / `stream` arguments in GPU code                 | Compare every call against its declaration (5d)                                  |
| Missing lifetime issues with temporaries                      | Trace each temporary's lifetime through async operations                         |
| Missing member shadowing that hides base class cleanup        | Compare member names against the base class (5b)                                 |
| Preferring the bundled KB over the checkout's own docs        | The checkout's `CODING_STYLE.md` and `.claude/CLAUDE.md` win (Step 1)            |
| Treating the deep review as another general review            | Narrow to bugs, logic, lifecycle, and architecture                               |
| Padding the output with nitpicks or praise                    | Proportionate findings; at most 1-2 sentences of "What's Good"                   |

---

# Quick Reference: Red Flags

Always 🔴:

- Bug fix without a failing-first test
- `VELOX_CHECK_*` for user errors (or `VELOX_USER_CHECK_*` for internal ones)
- Generic `VELOX_CHECK(expr)` instead of a comparison macro
- `goto` / `friend` / `FRIEND_TEST`
- Non-trivial logic in a `.h`
- `using namespace` in a header
- `*Utils` / `*Helpers` / `*Common` names
- `#define` macro where an inline function or template works
- Unrelated changes mixed into one change
- Performance claim without benchmarks
- Lifetime or ownership unclear
- `getOutput()` returning a member without `std::move()`
- A workaround that hides an infrastructure bug
- Test expectations updated to match buggy output
