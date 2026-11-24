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
  When something needs a git write (a stale base ref, say), tell the author the
  command instead of running it.
- Fixes happen only after the review, only for the findings the author picks,
  and only when asked. **Never commit or push**, even after fixes are applied.

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

A stale remote-tracking ref is fine as long as it is an ancestor of the branch;
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
`--base`.

The commit subjects here are also the closest thing to a PR title and
description — use them in Step 3.

### 2e. Build a changed-files manifest

Produce a manifest of changed files before starting the review:

| Category | Action |
|----------|--------|
| **New files** | Read the full file (mandatory) — includes untracked new files from 2c |
| **Deleted files** | Grep codebase for remaining references to symbols defined in deleted files |
| **Modified files** | Read the full file, not just changed hunks. Step 5a defines read depth and the large-file exception — follow it, do not apply a separate threshold here |

For deleted files, verify cleanup is complete:

```bash
# For each deleted header, check for stale includes or symbol references.
rg 'DeletedFileName' --type cpp --type-add 'cpp:*.h'
```

Note the size. If the change is well past ~400 changed lines, say so in the
summary — reviewers will likely ask for it to be split. [Source: `CONTRIBUTING.md`]

## Step 3: Classify Change Type

Determine the change type from the commit subjects (Step 2d) and the diff, and
apply the matching checklist from the KB (Section 7). Every change type has its
own checklist:

| Change Type | Key Checks |
|---------|------------|
| Bug fix | Failing-first test? Root cause? Edge cases? Correct CHECK family? Causation verified empirically? |
| Performance | Benchmark data? Before/after? Data-driven? Proportionate complexity? |
| New API | Minimal surface? Header docs? Impls in `.cpp`? No friend? No default args? No leaked internals? |
| Refactor | Separate from logic? Existing tests unchanged? No format churn mixed in? |
| New function | Upstream semantics? Fuzzer? `.rst` docs? Edge cases (null, max, type boundaries)? |
| Tests-only | Correct semantics? gtest matchers? Grouped by topic? Error paths? |
| Docs-only | Accurate? Code examples tested? Performance claims backed? |

If the change mixes types — a feature plus an unrelated fix — that is itself a
finding: reviewers will ask for separate PRs.

## Step 4: Two-Pass Deep Review

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

**Pass 2 — Common feedback (what Velox reviewers raise most, in order):**

6. **Readability** — simplify logic, suggest clearer alternatives
7. **Testing quality** — missing tests, edge cases, test readability,
   gtest matchers, `/*argName=*/value` format, no `TestValue`
8. **Code organization** — separate PRs, move logic to `.cpp`, extract
   shared code, reduce duplication
9. **Naming & style** — full descriptive names (not `val`/`idx`/`agg`),
   `numXxx` not `xxxCount`, no `*Utils`/`*Helpers`/`*Common`, remove unused
   headers/code
10. **Documentation** — `///` public API only, `//` everything else,
    active verbs, full sentences, no restating variable name

## Step 5: Read Surrounding Code

This is where agents fail most often. The diff alone hides critical context.
The full checkout is right here, so you MUST read surrounding code — not just
the changed hunks.

### 5a. Read full files, not just diffs

This step is the single source of truth for read depth. The Step 2e manifest
table defers to it — do not restate a threshold anywhere else.

Read the **entire file** for every new file and for every modified file. Full
reads are what surface shadowed members, lifetime bugs, and inconsistent
operator patterns, so this is the default rather than an aspiration.

Only when a file exceeds **1500 lines** may the read be reduced, and it must
then still cover:
- The class/struct declaration (all members, inheritance)
- The constructor and destructor
- Every method that appears in the diff
- 20-30 lines of context above/below each changed hunk

**Track coverage per region, never per file.** A reduced read leaves gaps, so
recording "read" against a filename is wrong and actively dangerous. Record what
was actually covered — `full file`, or `lines 1-140 plus addInput(), getOutput()`
— and treat everything outside that as unread.

**Re-read when in doubt.** Skip a read only when the exact region needed is
already recorded as covered. A duplicate read costs tokens; an unread base class
member or lifetime path costs a wrong review, and that failure is invisible in
the output. When the record is ambiguous, read again. A targeted re-read of a
specific region is likewise always correct, and expected, when confirming an
exact line number before citing it in a finding.

**Batch the manifest reads.** The Step 2e manifest is fully known before this
step begins, so issue all of its reads in one parallel batch rather than one
file at a time.

Batch only within this wave. The reads in 5b and 5c are *discovered* from what
5a reveals — which base class a new subclass extends, which sibling is worth
comparing against — so they form later, dependent waves. Collapsing them into a
single upfront batch would trade the read-then-reason loop that surfaces
shadowed members and inconsistent operator patterns for one undifferentiated
context dump.

**History is available.** The checkout has full history, so `git log`,
`git blame`, and merge-base comparisons work and are worth using to date a
regression or recover the intent behind a line.

### 5b. Base class verification (mandatory for new classes)

For each new class introduced, read its base class header **in full**. This step
is exempt from any coverage-based skipping: read the whole header even if 5a
already touched that file, and even if it exceeds the 1500-line threshold. This
is the step that catches member shadowing and virtual-override mismatches, and a
partial read is exactly how those are missed.

```
1. List all virtual methods — verify the subclass overrides the right ones.
2. List all member variables — check if any newly added member shadows a
   base class member (hides cleanup in close()/destructor).
3. Check close()/destructor patterns — does the base class expect subclasses
   to release resources in a specific order?
```

### 5c. Sibling pattern comparison

Read at least one similar component in the same directory to check
consistency. For operators: how do siblings handle `getOutput()` return
semantics, `isFinished()` state machine, stream synchronization? For
functions: how do nearby functions handle error paths, naming, signatures?

### 5d. Call-site signature verification

For every non-trivial function **called** in the diff (especially GPU code),
verify the call matches the declaration.

Collect the identifiers first and locate them in one pass rather than one `rg`
per function. Cap each pass at roughly 20-30 identifiers so the output stays
attributable:

```bash
rg -n --type cpp -e 'functionOne\(' -e 'functionTwo\(' -e 'functionThree\('
```

**Batch to discover, confirm to cite.** A batched pattern matches definitions,
declarations, and call sites indiscriminately, so a hit is a lead rather than a
verified signature. Before writing any finding that cites a signature, read that
specific declaration and confirm its argument list. Mis-attributing a hit to the
wrong overload produces a wrong finding, which costs far more than the greps it
saved.

Pay special attention to `mr` (memory resource) and `stream` arguments in
GPU/cudf code — missing these is a common and dangerous mistake.

### 5e. Test utility verification

Check if Velox-specific test macros exist for patterns used in test files:
- `VELOX_ASSERT_THROW` instead of raw `EXPECT_THROW`
- `VELOX_CHECK_EQ` family instead of `VELOX_CHECK(a == b)`
- gtest matchers (`ElementsAre`, `UnorderedElementsAre`, `IsEmpty`, `SizeIs`)

## Step 6: Cross-Check Against KB Red Flags

After both passes, explicitly verify against the Blocking Red Flags table from
the checklist. Every item must be checked. Key ones agents miss:

- Missing `mr` / `stream` arguments in GPU code
- Scope/lifetime issues with temporaries (especially before stream sync)
- Member variable shadowing base class members (hides cleanup in `close()`)
- Missing Velox test macros (`VELOX_ASSERT_THROW`, `VELOX_CHECK_EQ`)
- Inheritance pattern inconsistencies across similar operators
- `getOutput()` returning member without `std::move()` (breaks `isFinished()`)
- `#define` macros where inline functions or templates work
- Working around an infrastructure bug silently

Also check the Evidence Expectations table from KB Section 8 — does this change
provide the evidence its change type requires?

## Output Format

Output findings in this format. Assign each finding a stable ID (`F1`, `F2`,
...) sequentially. These IDs are how the author picks fixes afterwards.

```text
### Summary
<1-2 sentences. Verdict: ready to push / fix before pushing.>

### Critical Issues (🔴)
- **F1** [file:line] <Issue description>. Rule: <KB source>. Fix: <concrete suggestion>.

### Suggestions (🟡)
- **F2** [file:line] <Issue description>. <Why it matters>. <Concrete alternative>.

### Nitpicks (🟢)
- **F3** [file:line] <Issue description>.

### Positive Observations
<1-2 sentences on notable good patterns. Skip if nothing stands out.>
```

**Rules (from KB Section 9):**
- Cite exact file:line — never "somewhere in the code"
- Be actionable — suggest a specific fix, not a vague concern
- Skip what CI catches — formatting, include order, type errors
- No redundancy — each issue once
- Assume competence — explain Velox-specific context only
- Don't invent rules — when docs are silent, frame as question
- Proportionate — 3 critical issues > 20 nitpicks
- When an area falls under KB Section 10 (Unknowns), frame as question
- Finding IDs are sequential across severity levels (F1, F2, ... Fn)
- Keep findings short; skip filler ("it's worth noting", "presumably")
- No findings? Say so plainly. Do not invent nitpicks to fill the template

## After the Review

End by offering to apply fixes, e.g. *"Want me to fix F1 and F3?"* Then:

- Apply only the findings the author chooses.
- Re-read each region before editing it; the author may have changed it since.
- After applying, summarize what changed per finding ID. The author commits.

---

# Second Opinion Review Mode

Opt-in. Triggered by `--deep`, or when the user asks for `another review`,
`second opinion`, `deep review`, or `bugs/logic/architecture review`.

This launches a **separate subagent** focused on higher-risk findings that
the standard review might miss.

## Focus Areas

- Logic bugs, hidden edge cases, and invariant violations
- Memory safety, ownership, lifecycle, and state-machine issues
- Concurrency hazards, shared-state assumptions, and exception safety
- Code architecture: layering, cohesion, API boundaries, hidden coupling
- Missing tests only when they hide bug risk or architectural blind spots

**Deprioritize:** naming nits, comment wording, formatting, and minor style
feedback unless they hide a correctness or architecture problem.

## Subagent Rules

- Launch a **readonly** subagent for the second opinion review (Claude Code's
  Agent/Task tool, Cursor's subagents). If the environment cannot launch one,
  run the second pass yourself after the standard review, with only the focus
  areas above in mind.
- Use the current chat model for the subagent. The user is expected to have
  already switched to their preferred model (e.g., `gpt-5.4-xhigh`) before
  triggering this mode. Do NOT check or gate on model availability — just
  launch the subagent with whatever model is active.

## Prompt Shape

Provide the subagent with:
1. The change set and manifest (Step 2)
2. The bundled KB and checklist content
3. The standard review findings (so it can avoid duplicates)

```text
Review these local Velox changes as a second-opinion reviewer.

You have the standard review findings below. Focus on what they MISSED:
1. correctness and hidden logic bugs
2. lifetime, ownership, and state-machine invariants
3. concurrency, exception safety, and regression risk
4. code architecture, boundaries, and hidden coupling

Avoid style and naming nits unless they hide a bug or architectural problem.
Read surrounding code, not just the diff. Do not edit any file.
Return only NEW findings not already in the standard review.

Use 🔴/🟡 severity. Omit 🟢 unless the point affects correctness.

--- Standard Review Findings ---
<paste the standard review findings here>
```

## Merging

After the second opinion subagent returns, merge its findings with the standard review:
- Deduplicate identical issues
- Keep higher severity if both flagged the same thing
- Preserve file:line citations from both sources
- Continue the ID sequence; never renumber existing findings
- Mark second-opinion findings `(deep)` so the author can see their source

Then present the merged findings in the Output Format.

---


# Common Agent Failures

These are specific failures observed when agents review Velox changes without
this skill. Check yourself against each one:

| Failure | What to do instead |
|---------|--------------------|
| Reviewing only `git diff`, missing untracked new files | Include `new_source_files` (Step 2c) |
| Reviewing every untracked file | Filter to source files — `.cache/` alone can hold thousands (Step 2c) |
| Reviewing upstream commits as if they were the author's | Check commit authors since the merge base; a stale base ref causes this (Step 2d) |
| Finding the base by remote name (`upstream`, `origin`) | Match the `facebookincubator/velox` URL (Step 2b) |
| Running `git fetch` or editing files during the review | The review is read-only; fixes come after, when asked (Ground Rules) |
| Preferring the bundled KB over the checkout's own docs | The checkout's `CODING_STYLE.md` and `.claude/CLAUDE.md` win (Step 1) |
| Reading only diff hunks, not full files | Read the complete file for every new/modified file (Step 5a) |
| Reading manifest files one at a time | Issue all Step 2e manifest reads in one parallel batch (Step 5a) |
| Batching 5b/5c reads together with the 5a manifest | They are discovered from 5a — keep them as later, dependent waves (Step 5a) |
| Marking a file "read" after a reduced read | Coverage is per-region, not per-file; record the ranges actually read (Step 5a) |
| Skipping a base class header because 5a touched the file | 5b always reads the base class header in full, no exceptions (Step 5b) |
| Applying a read-depth threshold outside Step 5a | 5a is the single source of truth; 2e and 5b defer to it |
| Citing a signature straight from a batched grep hit | A hit is a lead; read the declaration and confirm the argument list first (Step 5d) |
| Not reading base class when reviewing a new subclass | Read the base class header: check virtuals, members, cleanup (Step 5b) |
| Not checking for stale references after file deletion | Grep for symbols from deleted files to verify cleanup (Step 2e) |
| Missing function arguments (`mr`, `stream` in GPU code) | Read every function call signature, compare against declaration (Step 5d) |
| Missing scope/lifetime issues with temporaries | Trace every temporary's lifetime through async operations |
| Missing member shadowing that hides base class cleanup | Check if any member variable name matches a base class member |
| Not using Velox-specific macros (`VELOX_ASSERT_THROW`, `VELOX_CHECK_EQ`) | Reference the KB Section 4.1 for CHECK patterns |
| Not asking architecture/design questions | Compare operator patterns across similar components (Step 5c) |
| Gating on model availability before second opinion | User already switched models; just launch the subagent |
| Treating second opinion as another general review | Narrow to bugs, logic, lifecycle, and architecture only |
| Too verbose — paragraphs of explanation | Be terse. Questions. Code suggestions. Done. |
| 5+ positive observations diluting signal | Max 1-2 sentences of praise |
| AI filler words ("presumably", "it's worth noting") | Delete them. State facts or ask questions. |



# Quick Reference: Velox Red Flags

From the checklist (always enforce):

- Bug fix without failing-first test → 🔴
- `VELOX_CHECK_*` for user errors → 🔴
- Generic `VELOX_CHECK(expr)` instead of comparison macro → 🔴
- `goto` / `friend` / `FRIEND_TEST` → 🔴
- Non-trivial logic in `.h` → 🔴
- `using namespace` in header → 🔴
- `*Utils` / `*Helpers` / `*Common` names → 🔴
- `#define` macro where inline function or template works → 🔴
- Unrelated changes mixed into one change → 🔴
- Performance claim without benchmarks → 🔴
- Lifetime/ownership unclear → 🔴
- `getOutput()` returning member without `std::move()` → 🔴
- Workaround that hides an infrastructure bug → 🔴
- Test expectations updated to match buggy output → 🔴
