# Velox Code Review Knowledge Base

## 1. Purpose and Scope

This is the reference an AI agent applies when reviewing **your own local Velox
changes before you push them**. It consolidates the repository's written rules,
the feedback reviewers most commonly give, and the project's stated values into
one actionable document.

The agent produces findings; you decide which to fix. So every finding must be
precise, cite its source, carry a severity, and never invent a rule the
repository does not have.

**This is not** a contributor onboarding guide or a build tutorial. It assumes
the code under review is C++ in a performance-critical database execution
engine, where bugs cause data corruption, crashes, or security problems.

---

## 2. Source Map

Every rule traces back to one of these files in the Velox repository. Citations
appear inline as `[Source: path]`.

| Source                                                    | Role                                                                  | Primary Type     |
| --------------------------------------------------------- | --------------------------------------------------------------------- | ---------------- |
| `CODING_STYLE.md`                                         | Canonical coding style, API, tests, comments, naming, headers, macros | Repository Rule  |
| `CONTRIBUTING.md`                                         | PR process, CI, tests, benchmarks, change sizing, function guidance   | Repository Rule  |
| `.claude/CLAUDE.md`                                       | Condensed Velox rules and recurring mistakes                          | Repository Rule  |
| `.claude/skills/pr-review/SKILL.md`                       | Review priorities, severity, comment quality                          | Review Heuristic |
| `website/docs/community/01-design-philosophy.md`          | Reliability, readability, data-driven performance, adaptivity         | Project Value    |
| `website/docs/community/02-technical-governance.md`       | Expert review, component ownership                                    | Review Heuristic |
| `website/docs/community/03-components-and-maintainers.md` | Which experts own which components                                    | Review Heuristic |

**The checkout under review is authoritative.** These files live in the Velox
repository you are reviewing, and they change over time. When a file above
exists in the checkout and disagrees with this document, the checkout wins:
this document is a snapshot, the repository is the rule.

### Label Key

| Label                | Meaning                                                   | Agent Behavior                              |
| -------------------- | --------------------------------------------------------- | ------------------------------------------- |
| **Repository Rule**  | Explicitly stated in repo docs                            | Flag as 🔴 Critical if violated             |
| **Review Heuristic** | Feedback reviewers give consistently, not a written rule  | Flag as 🟡 Suggestion                       |
| **Project Value**    | Directional principle from the design philosophy          | Frame as question or trade-off, not mandate |

When two sources overlap, prefer the more specific Repository Rule. Use Project
Values to interpret trade-offs, not to override explicit rules. When the docs
are silent, frame feedback as a question, not invented policy.
[Source: `01-design-philosophy.md`, `CONTRIBUTING.md`]

---

## 3. Review Priorities

A review should catch the **most dangerous** issues first, and then the issues
reviewers **most often** raise — because those are what will come back on your
PR if you do not fix them first.

### By Severity (what matters most if missed)

[Source: `.claude/skills/pr-review/SKILL.md`, `CONTRIBUTING.md`]

| Priority                           | Focus Areas                                                                                                                               |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Correctness & Edge Cases**    | Logic errors, off-by-one, null/empty handling, boundary conditions, integer overflow, floating-point edge cases (NaN, Inf, negative zero) |
| **2. Memory Safety & Lifetimes**   | Use-after-free, double-free, leaks, dangling pointers/references, buffer overflows, ownership issues, exception safety                    |
| **3. Concurrency & Thread-Safety** | Race conditions, data races, deadlocks, lock ordering, thread-safety of shared state, caches, memory pools, task scheduling               |
| **4. Performance & Allocations**   | Unnecessary copies (move semantics?), inefficient algorithms, cache-unfriendly access, excessive allocations in hot paths                 |
| **5. Error Handling**              | All error paths handled? Correct `VELOX_CHECK_*` vs `VELOX_USER_CHECK_*`? Informative messages? Runtime data at end?                     |
| **6. Code Quality & API Design**   | RAII, const-correctness, smart pointers, naming, small public API surface, encapsulation                                                  |
| **7. Testing Sufficiency**         | Edge cases covered? Error paths tested? Bug-fix tests fail without the fix? Gtest matchers?                                               |

### By How Often Reviewers Raise It

The themes below are listed from most to least common in Velox code review. The
order is the useful part: readability and test quality draw more comments than
anything else, so a change that is correct but hard to read will still come
back.

| Order | Theme                    | The question a reviewer will ask                                    |
| ----- | ------------------------ | ------------------------------------------------------------------- |
| 1     | Readability & clarity    | Could this be written more simply or more directly?                 |
| 2     | Testing quality          | Can a reader tell what this test verifies, and is the edge covered? |
| 3     | Correctness & edge cases | What happens when this is null, empty, or zero?                     |
| 4     | Code organization        | Is this an unrelated change that belongs in its own PR?             |
| 5     | Naming & style           | Is the name descriptive, and unabbreviated?                         |
| 6     | Performance              | Is something allocated on every call that could be reused?          |
| 7     | API design               | Does this method belong on this class, or somewhere else?           |
| 8     | Documentation            | Is this documented, and is the comment style right?                 |
| 9     | Error handling           | Should this be `VELOX_FAIL` or `VELOX_NYI`?                         |
| 10    | Memory safety            | Who owns this allocation, and when is it released?                  |

### How to Reconcile

For every change, scan for **severity-ordered** issues first — a correctness bug
always outranks a readability point. Then make a second pass for the
**common-feedback** themes. Both matter: a change with no bugs but consistent
readability problems is not ready by project standards.

The project explicitly values readability: *"Velox coding is optimized for the
reader, not the writer."* [Source: `01-design-philosophy.md`, `CODING_STYLE.md`]

---

## 4. Hard Rules from Repository Docs

### 4.1 Asserts and CHECKs

| Rule                                                                                                                                                 | Type            | Source                         |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | --------------- | ------------------------------ |
| Use `VELOX_CHECK_*` for internal errors, `VELOX_USER_CHECK_*` for user errors                                                                        | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Prefer two-argument forms: `VELOX_CHECK_LT(idx, size)` over `VELOX_CHECK(idx < size)` — prints both values on failure                                | Repository Rule | `CODING_STYLE.md`              |
| Use `VELOX_FAIL()` / `VELOX_USER_FAIL()` for unconditional throws                                                                                    | Repository Rule | `CODING_STYLE.md`              |
| Use `VELOX_UNREACHABLE()` for impossible branches, `VELOX_NYI()` for unimplemented paths                                                             | Repository Rule | `CODING_STYLE.md`              |
| Put runtime information at the **end** of error messages, after the static description: `"Column is ambiguous: {}"` not `"Column '{}' is ambiguous"` | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |

### 4.2 Comments

| Rule                                                                                                     | Type            | Source                               |
| -------------------------------------------------------------------------------------------------------- | --------------- | ------------------------------------ |
| Use `///` **only** for public API in headers (classes, public methods, public members)                   | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`       |
| Use `//` for everything else: private/protected members, anonymous-namespace items, code-block internals | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`       |
| Start comments with active verbs, not "This class..." or "This method..."                                | Repository Rule | `CODING_STYLE.md`                    |
| Comments MUST be full English sentences: capital letter, period at end                                   | Repository Rule | `CODING_STYLE.md`                    |
| Comment every file, every class, every non-trivial method, every member variable                         | Repository Rule | `CODING_STYLE.md`, `CONTRIBUTING.md` |
| Do not duplicate comments between `.h` and `.cpp` — document in header only                              | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`       |
| Do not restate the variable name; explain semantic meaning or omit                                       | Repository Rule | `CLAUDE.md`                          |
| Comments explain *why*, not *what* — avoid redundant narration                                           | Repository Rule | `CODING_STYLE.md`                    |
| TODO format: `// TODO: Description.` — no author username                                                | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`       |

### 4.3 Naming Conventions

| Convention                                                                                        | Applies To                                     | Source                         |
| ------------------------------------------------------------------------------------------------- | ---------------------------------------------- | ------------------------------ |
| **PascalCase**                                                                                    | Types, file names                              | `CODING_STYLE.md`              |
| **camelCase**                                                                                     | Functions, member/local variables              | `CODING_STYLE.md`              |
| **camelCase_**                                                                                    | Private/protected member variables             | `CODING_STYLE.md`              |
| **snake_case**                                                                                    | Namespace names, build targets                 | `CODING_STYLE.md`              |
| **UPPER_SNAKE_CASE**                                                                              | Macros                                         | `CODING_STYLE.md`              |
| **kPascalCase**                                                                                   | Static constants, enumerators                  | `CODING_STYLE.md`              |
| **testing** prefix                                                                                | Test-only class methods (e.g., `testingFoo()`) | `CODING_STYLE.md`              |
| **debug** prefix                                                                                  | Debug-only query configs                       | `CODING_STYLE.md`              |
| MUST NOT abbreviate names; use full descriptive names                                             | Repository Rule                                | `CLAUDE.md`, `CODING_STYLE.md` |
| Well-established abbreviations acceptable: `id`, `url`, `sql`, `expr`; loop indices `i`, `j`, `k` | Repository Rule                                | `CODING_STYLE.md`              |
| Prefer `numXxx` over `xxxCount` (e.g., `numRows`)                                                 | Repository Rule                                | `CLAUDE.md`, `CODING_STYLE.md` |
| MUST NOT name files or classes `*Utils`, `*Helpers`, `*Common`                                    | Repository Rule                                | `CLAUDE.md`, `CODING_STYLE.md` |

### 4.4 Variables and Constants

| Rule                                                                                 | Type            | Source                         |
| ------------------------------------------------------------------------------------ | --------------- | ------------------------------ |
| Prefer `std::string_view` over `const std::string&` for function parameters          | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Use uniform initialization: `size_t size{0}` not `size_t size = 0`                   | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Declare variables in the smallest scope, close to usage                              | Repository Rule | `CODING_STYLE.md`              |
| Use digit separators for numbers with 4+ digits: `10'000`                            | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Use trailing commas in multi-line initializer lists, enums, and argument lists       | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Prefer value types > `std::optional` > `std::unique_ptr` (in that order)             | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md` |
| Use `std::string_view` for constant string literals, never `std::string` (SIOF risk) | Repository Rule | `CODING_STYLE.md`              |
| Use `std::optional` instead of `folly::Optional` / `boost::optional`                 | Repository Rule | `CODING_STYLE.md`              |
| Do not use `folly::StringPiece` in new code; use `std::string_view`                  | Repository Rule | `CODING_STYLE.md`              |

### 4.5 API Design

| Rule                                                                                    | Type            | Source                                            |
| --------------------------------------------------------------------------------------- | --------------- | ------------------------------------------------- |
| Keep the public API surface small and intentional                                       | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`, `CONTRIBUTING.md` |
| Prefer free functions in `.cpp` (anonymous namespace) over private/static class methods | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| Define free functions close to where they are used                                      | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| Keep non-trivial implementations in `.cpp`; only trivial one-liners in headers          | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| Avoid default arguments when all callers can pass values explicitly                     | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| MUST NOT use `friend`, `FRIEND_TEST`, or any friend declarations                        | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| MUST NOT use `goto`                                                                     | Repository Rule | `CLAUDE.md`, `CODING_STYLE.md`                    |
| Do not use macros unless absolutely necessary; prefer inline functions or templates     | Repository Rule | `CODING_STYLE.md`                                 |

### 4.6 Headers and Includes

| Rule                                                          | Type             | Source                   |
| ------------------------------------------------------------- | ---------------- | ------------------------ |
| Use `#pragma once` for inclusion guards                       | Repository Rule  | `CODING_STYLE.md`        |
| Includes use full path relative to repo root                  | Repository Rule  | `CODING_STYLE.md`        |
| Forward-declare in `.h` when only pointer/reference is needed | Repository Rule  | `CODING_STYLE.md`        |
| Never put `using namespace` or `using X::Y` in a header file  | Repository Rule  | `CODING_STYLE.md`        |
| Do not use `using namespace std;`                             | Repository Rule  | `CODING_STYLE.md`        |
| Remove unused headers                                         | Review Heuristic | Common review feedback   |

### 4.7 Tests

| Rule                                                                                                  | Type             | Source                         |
| ----------------------------------------------------------------------------------------------------- | ---------------- | ------------------------------ |
| Every change MUST include unit tests covering the logic added/modified (rare exceptions)              | Repository Rule  | `CONTRIBUTING.md`              |
| Bug fixes MUST include a test that fails without the fix                                              | Repository Rule  | `CLAUDE.md`, `CODING_STYLE.md` |
| Do not update test expectations to match buggy behavior without verifying correctness                 | Repository Rule  | `CLAUDE.md`, `CODING_STYLE.md` |
| Place new tests next to related existing tests, not at end of file; group by topic                    | Repository Rule  | `CLAUDE.md`, `CODING_STYLE.md` |
| Use gtest container matchers (`ElementsAre`, `UnorderedElementsAre`, `Contains`, `IsEmpty`, `SizeIs`) | Repository Rule  | `CLAUDE.md`, `CODING_STYLE.md` |
| New scalar functions MUST be fuzzed with `velox_expression_fuzzer_test`                               | Repository Rule  | `CONTRIBUTING.md`              |
| Avoid `TestValue` unless absolutely necessary — test through public API                               | Review Heuristic | Common review feedback         |
| Use `/*argName=*/value` format for constant parameters in calls                                       | Review Heuristic | Common review feedback         |

### 4.8 PR Conventions

These apply to the PR you are about to open. Checking them before you push is
cheaper than fixing them after review.

| Rule                                                                             | Type             | Source                         |
| -------------------------------------------------------------------------------- | ---------------- | ------------------------------ |
| PR title format: `<type>[(scope)]: <description>` following conventional commits | Repository Rule  | `CONTRIBUTING.md`              |
| Valid types: `feat`, `fix`, `perf`, `build`, `test`, `docs`, `refactor`, `misc`  | Repository Rule  | `CONTRIBUTING.md`              |
| Breaking changes: append `!` to type and add `BREAKING CHANGE:` footer           | Repository Rule  | `CONTRIBUTING.md`              |
| Format-only changes MUST be in separate PRs from logic changes                   | Repository Rule  | `CODING_STYLE.md`              |
| Unrelated changes belong in separate PRs                                         | Review Heuristic | Common review feedback         |
| Do not ignore red CI signals, even if they seem preexisting                      | Repository Rule  | `CONTRIBUTING.md`              |
| Keep PRs small (effective review drops past ~400 lines)                          | Repository Rule  | `CONTRIBUTING.md`              |
| Do not silently work around shared infrastructure or CI bugs                     | Repository Rule  | `CLAUDE.md`, `CODING_STYLE.md` |

---

## 5. Do's and Don'ts

### Do

- **First pass: scan for safety and correctness bugs.** Off-by-one, null
  dereference, use-after-free, data race, wrong error classification. These are
  always 🔴 Critical. [Source: `.claude/skills/pr-review/SKILL.md`]
- **Second pass: scan for readability and common-feedback themes.** Simplify
  logic, flag duplication, suggest clearer alternatives. Readability draws more
  review comments than any other theme. Offer concrete rewrites, not vague
  concerns. [Source: `01-design-philosophy.md`]
- **Verify tests exist and cover edge cases.** Check null, empty, boundary,
  overflow, negative inputs. [Source: `CONTRIBUTING.md`]
- **Verify bug-fix tests fail without the fix.** A test that passes both ways
  proves nothing. [Source: `CLAUDE.md`, `CODING_STYLE.md`]
- **Separate unrelated changes.** Mixed concerns are among the most consistent
  things reviewers ask to split out. [Source: `CODING_STYLE.md`]
- **Flag code duplication and suggest extraction** into a shared function.
- **Flag unused headers and dead code.**
- **Confirm performance claims have benchmark data.**
  [Source: `CONTRIBUTING.md`, `01-design-philosophy.md`]
- **Check `CMakeLists.txt` entries are in alphabetical order.**
- **For specialized components, note which experts should review.**
  [Source: `02-technical-governance.md`, `03-components-and-maintainers.md`]

### Don't

- **Don't comment on formatting that CI or clang-format would catch.** Spend
  attention on what automated tools cannot detect.
  [Source: `.claude/skills/pr-review/SKILL.md`]
- **Don't suggest `friend` or `FRIEND_TEST`.** Always banned.
  [Source: `CLAUDE.md`, `CODING_STYLE.md`]
- **Don't substitute personal taste for a repo rule.** When docs are silent,
  phrase feedback as a question or trade-off, not a mandate.
  [Source: `01-design-philosophy.md`, `CONTRIBUTING.md`]
- **Don't use `TestValue` for testing** unless no alternative exists — it tests
  implementation details rather than behavior, and breaks under refactoring.
- **Don't use `#define` when inline functions or templates work.**
  [Source: `CODING_STYLE.md`]
- **Don't accept unexplained public API growth** or non-trivial header
  implementations just because they are convenient.
  [Source: `CODING_STYLE.md`, `CLAUDE.md`]

---

## 6. Common Mistakes and Red Flags

### Blocking (🔴 — fix before you push)

| Red Flag                                                                    | Why It Blocks                                                                             | Source                                                             |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Bug fix without a failing-first test                                        | Proves the test actually catches the bug                                                  | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Logic change without tests or verification story                            | Tests required for every change                                                           | `CONTRIBUTING.md`                                                  |
| `VELOX_CHECK_*` for user errors or `VELOX_USER_CHECK_*` for internal errors | Wrong error category confuses users                                                       | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Generic `VELOX_CHECK(expr)` where comparison macro should be used           | Loses operand values in failure messages                                                  | `CODING_STYLE.md`                                                  |
| Use of `goto`                                                               | Banned unconditionally                                                                    | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Use of `friend` / `FRIEND_TEST`                                             | Banned unconditionally                                                                    | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Non-trivial logic in header files                                           | Increases build time, violates style                                                      | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Test expectations updated to match buggy output                             | Masks real bugs                                                                           | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| `using namespace` in a header file                                          | Pollutes global namespace                                                                 | `CODING_STYLE.md`                                                  |
| File/class named `*Utils`, `*Helpers`, `*Common`                            | Attracts unrelated functions, loses cohesion                                              | `CLAUDE.md`, `CODING_STYLE.md`                                     |
| Macros where inline functions or templates suffice                          | Macros banned unless necessary                                                            | `CODING_STYLE.md`                                                  |
| Unrelated changes mixed into one PR                                         | Pollutes review, makes bisecting impossible                                               | `CODING_STYLE.md`                                                  |
| Performance claim without benchmark data                                    | Data-driven optimization required                                                         | `CONTRIBUTING.md`, `01-design-philosophy.md`                       |
| New function missing upstream semantics validation, docs, or fuzzing        | Function contribution requirements not met                                                | `CONTRIBUTING.md`                                                  |
| Lifetime/ownership unclear and may dangle or race                           | Safety-critical in C++                                                                    | `.claude/skills/pr-review/SKILL.md`                                |
| Workaround that hides an infrastructure or CI bug                           | CI integrity required                                                                     | `CONTRIBUTING.md`, `CLAUDE.md`                                     |
| Operator `getOutput()` returns member (e.g. `input_`) without `std::move()` | Leaves member non-null, breaks `isFinished()` state check, causes infinite driver polling | [PR #17150](https://github.com/facebookincubator/velox/pull/17150) |

### Non-Blocking (🟡 — worth fixing before review)

| Issue                                                         | Source                               |
| ------------------------------------------------------------- | ------------------------------------ |
| Code could be simplified or made more readable                | Common review feedback               |
| Missing edge case coverage in tests (null, empty, boundary)   | `CONTRIBUTING.md`                    |
| Unused code, headers, or dead imports                         | Common review feedback               |
| Code duplication that could be extracted to a shared function | Common review feedback               |
| Hard-coded test values without explanation                    | Common review feedback               |
| Abbreviated variable names (e.g., `val`, `idx`, `agg`)        | `CLAUDE.md`, `CODING_STYLE.md`       |
| `///` used on non-public items                                | `CLAUDE.md`, `CODING_STYLE.md`       |
| `const std::string&` where `std::string_view` would suffice   | `CLAUDE.md`, `CODING_STYLE.md`       |
| Tests appended at end of file instead of near related tests   | `CLAUDE.md`, `CODING_STYLE.md`       |
| `CMakeLists.txt` entries not in alphabetical order            | Common review feedback               |
| Missing `/*argName=*/value` format for constant test params   | Common review feedback               |
| Missing trailing comma in multi-line list                     | `CLAUDE.md`, `CODING_STYLE.md`       |
| Assignment initialization instead of uniform initialization   | `CLAUDE.md`, `CODING_STYLE.md`       |
| Missing digit separators on large literals                    | `CLAUDE.md`, `CODING_STYLE.md`       |
| Comment restates the variable name without adding meaning     | `CLAUDE.md`                          |
| Multiple individual assertions instead of gtest matchers      | `CLAUDE.md`, `CODING_STYLE.md`       |
| Mix of format churn and logic in same PR                      | `CODING_STYLE.md`, `CONTRIBUTING.md` |

---

## 7. Change-Type-Specific Checklists

### Bug Fixes

- Is there a test that fails without the fix and passes with it?
- Does the fix address the root cause, not just the symptom?
- Has causation been verified empirically, not just from commit messages?
- Are related edge cases covered (null, empty, boundary, overflow)?
- Is `VELOX_CHECK_*` vs `VELOX_USER_CHECK_*` used correctly?
- Is the error message informative with runtime data at the end?
- Were expectations validated against correct behavior (not buggy output)?
- Are there unrelated changes that should be in a separate PR?

### Performance Optimizations

- Are before/after benchmark results ready for the PR description?
- Are benchmarks based on realistic workloads, not synthetic micro-cases?
- Does the change avoid unnecessary copies? Are move semantics used?
- Are there new allocations in hot paths? If so, are they justified?
- Is the optimization data-driven? [Source: `01-design-philosophy.md`]
- Does it add a configuration knob that could be handled adaptively?
- Is the added complexity proportionate to the measured gain?
- Are there regressions in other code paths?
- Is the change isolated from unrelated refactors?

### New Public API

- Is the API surface minimal and intentionally public?
- Are all public items documented with `///` comments?
- Are implementations in `.cpp`, not `.h` (except trivial one-liners)?
- Could private helpers be free functions in `.cpp` instead?
- Are there no default arguments that callers can pass explicitly?
- No `friend` declarations?
- Are parameter types correct (`std::string_view` over `const std::string&`)?
- Is the naming convention correct (PascalCase types, camelCase functions)?
- Does the API leak internal structure or testing-only seams?

### Refactors

- Is the refactor separate from logic changes?
- Do existing tests still pass without modification?
- If tests were modified, is the reason clearly justified?
- No behavior changes introduced accidentally?
- Is code duplication reduced, not increased?
- Does the diff avoid mixing format churn with logic?

### New Functions (Presto/Spark)

- Is the official function documentation linked?
- Are semantics verified against the reference engine (Spark 3.5 ANSI OFF for Spark)?
- Are edge cases tested (null, empty, max values, type boundaries)?
- Was the expression fuzzer run? [Source: `CONTRIBUTING.md`]
- Is documentation added to the `.rst` file under `velox/docs/functions/`?
- Are there any `#define` macros that should be inline functions?
- Are obvious inefficiencies addressed and benchmarked if they matter?

### Tests-Only Changes

- Do the tests verify correct semantics, not just current implementation?
- Are gtest matchers used for collection assertions?
- Are tests grouped by topic, not appended at end of file?
- Are error paths and edge cases covered, not just happy paths?
- Are hard-coded values explained or given named constants?
- Are tests fast, isolated, and repeatable?

### Docs-Only Changes

- Is the documentation accurate and consistent with current code?
- Are code examples correct and tested?
- If docs describe new semantics, should code or tests also change?
- Are any performance or compatibility claims backed by evidence?

---

## 8. Evidence Expectations

What a reviewer will ask your PR to show. Have it ready before you push.

| Change Type     | Evidence to Have Ready                                                | Source                                       |
| --------------- | --------------------------------------------------------------------- | -------------------------------------------- |
| Bug fix         | Regression test that fails before fix, passes after                   | `CLAUDE.md`, `CODING_STYLE.md`               |
| New feature     | Happy path + edge cases + error paths                                 | `CONTRIBUTING.md`                            |
| New function    | Upstream semantics validation + corner cases + fuzzer + `.rst` docs   | `CONTRIBUTING.md`                            |
| Performance     | Micro-benchmark or representative workload evidence with before/after | `CONTRIBUTING.md`, `01-design-philosophy.md` |
| Refactor        | Existing tests pass unchanged (or changes justified)                  | `CONTRIBUTING.md`                            |
| New/changed API | Intentional public surface + header docs + encapsulation rationale    | `CODING_STYLE.md`, `CONTRIBUTING.md`         |
| CI anomalies    | Green CI, or explicit link to a preexisting broken-CI issue           | `CONTRIBUTING.md`                            |

### When to Suggest Benchmarks

- The change claims a performance improvement without data.
- A new algorithm replaces an existing one in a hot path.
- A change adds allocations or copies in performance-sensitive code.

### When to Suggest Fuzzers

- New scalar functions (expression fuzzer is mandatory).
- Changes to expression evaluation, type casting, or serialization logic.
- Any change to input validation or parsing.

---

## 9. Output Format and Finding Discipline

### Finding Structure

Each finding answers three questions:

1. **What** is wrong (exact file and line)
2. **Why** it matters in Velox (risk or rule reference)
3. **What** concrete fix resolves it

### Severity Levels

| Label             | Meaning                 | When to Use                                                                 |
| ----------------- | ----------------------- | --------------------------------------------------------------------------- |
| 🔴 **Critical**   | Fix before you push     | Correctness bugs, safety issues, data integrity, Repository Rule violations |
| 🟡 **Suggestion** | Should fix before review| Performance, maintainability, clarity, Review Heuristic items               |
| 🟢 **Nitpick**    | Minor                   | Naming, style that CI missed, minor readability                             |

### Discipline Rules

- **Cite exact files and lines** — never "somewhere in the code"
- **Be actionable** — a specific fix, not a vague concern
- **Skip what CI catches** — formatting, include order, type errors, unless they
  hide a real bug
- **No redundancy** — each issue once
- **Assume competence** — the author knows C++; explain Velox-specific context only
- **Don't invent rules** — if it is not in the repo docs, frame it as a heuristic
  or a question
- **Proportionate** — 2-3 critical issues are more useful than 20 nitpicks

---

## 10. Unknowns and Human Judgment

These areas are **not** governed by explicit repo rules. Frame feedback as
suggestions or questions, not rule violations.

| Area                                | What the Docs Say                    | What They Don't Say                                              |
| ----------------------------------- | ------------------------------------ | ---------------------------------------------------------------- |
| **`shared_ptr` vs `unique_ptr`**    | Prefer value > optional > unique_ptr | When shared_ptr is appropriate for shared ownership              |
| **Template metaprogramming limits** | No specific guidance                 | How much template complexity is acceptable                       |
| **Exception safety guarantees**     | Review guidance mentions it          | Strong vs basic guarantee expectations                           |
| **Logging level conventions**       | No documented policy                 | When to use LOG(INFO) vs LOG(WARNING) vs LOG(ERROR)              |
| **Lock granularity and ordering**   | Check for deadlocks                  | No documented lock hierarchy                                     |
| **Test file structure**             | Tests in `tests/` subdirectories     | One test class per file vs multiple                              |
| **Benchmark thresholds**            | Benchmarks expected for perf claims  | No threshold for "significant" improvement                       |
| **Code coverage targets**           | Tests are required                   | No numeric coverage target                                       |
| **When `TestValue` is justified**   | Reviewers discourage it              | No formal policy                                                 |
| **Virtual vs non-virtual design**   | API surface small                    | No guidance on when to make methods virtual                      |
| **Component-specific invariants**   | Local code, tests, or design docs    | Memory, exec, vector, connector, fuzzer areas need local context |

**Project Value**: Reliability should beat cleverness. When an optimization or
abstraction adds complexity, ask whether the measured benefit justifies the
maintenance cost. [Source: `01-design-philosophy.md`]
