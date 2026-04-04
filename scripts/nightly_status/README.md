# Automated Nightly Status Reporting with Slack Integration

Automated nightly CI status reporting system that generates failure reports with AI-powered root cause analysis and posts them to Slack.

## What's included

### Workflow: `.github/workflows/nightly-status-report.yml`

- Runs daily at 11:00 UTC (after all nightly jobs complete) or manually via `workflow_dispatch`
- Supports optional `skip_rca` input
- Installs `gh` CLI and Claude CLI without `sudo` (distro-agnostic)
- Generates a nightly status report and upstream RCA report as Slack Block Kit JSON payloads
- Posts payloads to Slack via incoming webhooks

### Scripts

#### `check_nightly_status.py`

Fetches all nightly workflow runs from GitHub, builds a status summary table, and for each failure:

- Extracts job-level logs with fuzzy job name matching and `--log` fallback
- Uses Claude CLI (or NVIDIA LLM) to identify the root cause and suggest fixes
- AI extracts the 3-5 most relevant stacktrace lines (not the full log)
- Searches upstream repos for related issues/PRs
- Outputs a Slack Block Kit JSON payload directly

**Usage:**

```bash
python scripts/nightly_status/check_nightly_status.py [OPTIONS]
```

Generates `status-payload.json` (configurable via `STATUS_FILE` env var) — a Slack Block Kit JSON payload containing the status summary table and failure details with AI-analyzed stacktraces, causes, and fixes.

| Option | Description |
|---|---|
| `--no-cause` | Disable AI cause analysis (enabled by default) |
| `--no-fix` | Disable AI fix suggestions (enabled by default) |
| `--no-claude` | Use NVIDIA LLM instead of Claude for analysis |
| `--print-logs` | Print full failed log tails |

**Environment variables:**

| Variable | Description |
|---|---|
| `REPO` | Target repo (default: auto-detect via `gh`) |
| `ANTHROPIC_API_KEY` | API key for Claude CLI |
| `NVIDIA_API_KEY` / `LLM_API_KEY` | API key for NVIDIA LLM |
| `LLM_MODEL` | LLM model name |
| `LLM_API_URL` | LLM API endpoint |
| `STATUS_FILE` | Output file path (default: `status-payload.json`) |
| `GH_RETRIES` | Number of retries for GitHub API calls (default: 5) |
| `GH_HTTP_TIMEOUT` | HTTP timeout for GitHub API calls (default: 60) |

#### `search_upstream_issues.py`

Parses the status report, extracts error-specific search queries (test names, symbols, archive names), and searches upstream GitHub repos for related issues and PRs.

- **Presto failures** → searches `prestodb/presto` + `facebookincubator/velox`
- **Velox failures** → searches `facebookincubator/velox` only
- Filters results to the last 30 days only
- Outputs a Slack Block Kit JSON payload with the RCA report

**Usage:**

```bash
python scripts/nightly_status/search_upstream_issues.py -i status-payload.json -o rca-payload.json
```

| Option | Description |
|---|---|
| `-i`, `--input` | Path to status payload JSON (default: `status-payload.json`) |
| `-o`, `--output` | Path to write the RCA payload JSON (default: `rca-payload.json`) |
| `--days N` | Search issues/PRs created within the last N days (default: 30) |
| `--max-results N` | Max results per search query per repo (default: 5) |
| `--repos REPO [...]` | Additional repos to search |

## Required GitHub Secrets

| Secret | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Claude CLI authentication for AI analysis |
| `SLACK_WEBHOOK_NIGHTLY_STATUS_URL` | Incoming webhook URL for posting status and RCA reports to Slack |

## Manual Trigger

```bash
# Run with defaults
gh workflow run "Nightly Status Report" --ref nightly-status

# Skip upstream RCA search
gh workflow run "Nightly Status Report" --ref nightly-status -f skip_rca=true
```
