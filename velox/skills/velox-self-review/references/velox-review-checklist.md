# Velox Code Review Checklist

> The short form of `velox-review-kb.md`, for reviewing your own changes before
> you push. Sources are the Velox repository's own docs (`CODING_STYLE.md`,
> `CONTRIBUTING.md`, `.claude/CLAUDE.md`) and the feedback Velox reviewers most
> commonly give. Where a doc in the checkout disagrees with this file, the
> checkout wins.

---

## Two-Pass Review Approach

**Pass 1 — Severity (catch the dangerous):**

1. **Correctness** — logic errors, off-by-one, null/empty/boundary, overflow, NaN/Inf
2. **Memory safety** — use-after-free, dangling refs, leaks, ownership, exception safety
3. **Concurrency** — races, deadlocks, lock ordering, shared state under concurrent access
4. **Error handling** — correct CHECK/USER_CHECK, comparison macros, runtime data at end
5. **Performance** — unnecessary copies, hot-path allocations, missing benchmarks

**Pass 2 — Common feedback (what reviewers raise most, in order):**

6. **Readability** — simplify logic, suggest clearer alternatives
7. **Testing quality** — missing tests, edge cases, test readability
8. **Code organization** — separate PRs, move logic to `.cpp`, extract shared code
9. **Naming & style** — descriptive names, remove unused code/headers
10. **Documentation** — missing docs on public API, wrong `///` vs `//`

---

## Blocking Red Flags — 🔴 Fix Before You Push

| Flag                                                                 | Source                                       |
| -------------------------------------------------------------------- | -------------------------------------------- |
| Bug fix without a test that fails before the fix                     | `CLAUDE.md`, `CODING_STYLE.md`               |
| Logic change without tests or verification story                     | `CONTRIBUTING.md`                            |
| `VELOX_CHECK_*` for user errors or `VELOX_USER_CHECK_*` for internal | `CLAUDE.md`, `CODING_STYLE.md`               |
| Generic `VELOX_CHECK(expr)` where comparison macro should be used    | `CODING_STYLE.md`                            |
| `goto` anywhere                                                      | `CLAUDE.md`, `CODING_STYLE.md`               |
| `friend` / `FRIEND_TEST`                                             | `CLAUDE.md`, `CODING_STYLE.md`               |
| Non-trivial logic in `.h` (> 1 statement in method body)             | `CLAUDE.md`, `CODING_STYLE.md`               |
| Test expectations updated to match buggy output                      | `CLAUDE.md`, `CODING_STYLE.md`               |
| `using namespace` in a header file                                   | `CODING_STYLE.md`                            |
| File/class named `*Utils` / `*Helpers` / `*Common`                   | `CLAUDE.md`, `CODING_STYLE.md`               |
| Unrelated changes mixed into one PR                                  | `CODING_STYLE.md`                            |
| `#define` macro where inline function or template works              | `CODING_STYLE.md`                            |
| Performance claim without benchmark data                             | `CONTRIBUTING.md`, `01-design-philosophy.md` |
| Lifetime/ownership unclear, may dangle or race                       | `.claude/skills/pr-review/SKILL.md`          |
| Workaround that silently hides an infrastructure or CI bug           | `CONTRIBUTING.md`, `CLAUDE.md`               |

---

## Must-Check Test Expectations

- [ ] Bug fix? → Test MUST fail without the fix, pass with it
- [ ] New feature? → Happy path + edge cases (null, empty, max, overflow) + error paths
- [ ] New function? → All signatures + corner cases + fuzzer run + `.rst` docs + upstream validation
- [ ] Refactor? → Existing tests pass unchanged (or changes justified)
- [ ] Performance? → Before/after benchmarks ready for the PR description + existing tests unchanged

---

## Common Velox-Specific Traps

| Trap (wrong way)                       | Correct Pattern                                  | Type      |
| -------------------------------------- | ------------------------------------------------ | --------- |
| `VELOX_CHECK(a < b)`                   | `VELOX_CHECK_LT(a, b)` — prints both values      | Rule      |
| `"Column '{}' is ambiguous"`           | `"Column is ambiguous: {}"` — runtime data last  | Rule      |
| `///` on anonymous-namespace or private items | `//` — triple-slash is public API only    | Rule      |
| `const std::string&` parameter         | `std::string_view` preferred                     | Rule      |
| `size_t size = 0`                      | `size_t size{0}` — uniform initialization        | Rule      |
| `10000` (large literal)                | `10'000` — digit separators for 4+ digits        | Rule      |
| Abbreviated names: `val`, `idx`, `agg` | Full: `value`, `index`, `aggregation`            | Rule      |
| `class FooUtils`                       | Name after the concept: `class FooProjection`    | Rule      |
| Comment: "This class manages..."       | "Manages..." — start with an active verb         | Rule      |
| Tests appended at end of file          | Place next to related existing tests             | Rule      |
| Multiple `EXPECT_EQ` on a collection   | `EXPECT_THAT(result, ElementsAre(...))`          | Rule      |
| `#define MACRO(x)` for simple logic    | `inline` function or template                    | Rule      |
| `folly::StringPiece` in new code       | `std::string_view` instead                       | Rule      |
| `std::string` for constant literals    | `std::string_view` (SIOF risk)                   | Rule      |
| Duplicated code across components      | Extract a common function                        | Heuristic |
| Unrelated fix mixed into a feature     | Separate PR for each concern                     | Heuristic |
| Unused headers left in file            | Remove unused `#include` directives              | Heuristic |
| `TestValue` for behavior testing       | Test through the public API instead              | Heuristic |
| Constant param: `func(0, 2)`           | `func(/*offset=*/0, /*length=*/2)`               | Heuristic |
| `CMakeLists.txt` in random order       | Alphabetical order                               | Heuristic |
| Hard-coded test values unexplained     | Named constants or inline comments               | Heuristic |

---

## Evidence to Have Ready

| Change Type        | Have Ready                                                   |
| ------------------ | ------------------------------------------------------------ |
| Bug fix            | Failing test before fix, passing after                       |
| Logic change       | Unit tests: happy path + edge cases + error paths            |
| Performance        | Before/after benchmark with a representative workload        |
| New function       | Upstream semantics check + corner-case tests + docs + fuzzer |
| New API            | Justification for the public surface + header docs           |
| Component-specific | Who the component's maintainers are, to request their review |

---

## Output Format

```text
### Summary
<1-2 sentences: what the change does, and whether it is ready to push.>

### Critical Issues (🔴)
- **F1** [file:line] Issue. Risk: why. Fix: concrete change.

### Suggestions (🟡)
- **F2** [file:line] Issue. Why it matters. Concrete alternative.

### Nitpicks (🟢)
- **F3** [file:line] Issue.

### What's Good
<1-2 sentences on a specific pattern worth keeping. Skip if nothing stands out.>
```

### Finding Rules

- **Cite exact file:line** — never "somewhere in the code"
- **Be actionable** — a specific fix, not a vague concern
- **Skip what CI catches** — formatting, include order, type errors
- **No redundancy** — each issue once
- **Assume competence** — explain Velox-specific context only
- **Don't invent rules** — if the docs are silent, frame it as a question
- **Proportionate** — 3 critical issues > 20 nitpicks
