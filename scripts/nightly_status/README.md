# Automated Nightly Status Reporting with Slack Integration

Automated nightly CI status reporting system that generates failure reports with AI-powered root cause analysis and posts them to Slack.

## What's included

### Workflow: `.github/workflows/nightly-status-report.yml`

- Runs daily at 11:00 UTC (after all nightly jobs complete) or manually via `workflow_dispatch`
- Supports optional `date` and `skip_rca` inputs
- Installs `gh` CLI and Claude CLI without `sudo` (distro-agnostic)
- Generates a nightly status report and upstream RCA report
- Prepares Slack Block Kit payloads and posts to separate channels via incoming webhooks

### Scripts

#### `check_nightly_status.py`

Fetches all nightly workflow runs from GitHub, builds a status summary table, and for each failure:

- Extracts job-level logs with fuzzy job name matching and `--log` fallback
- Uses Claude CLI (or NVIDIA LLM) to identify the root cause and suggest fixes
- AI extracts the 3-5 most relevant stacktrace lines (not the full log)
- Searches upstream repos for related issues/PRs
- Outputs Slack-formatted `mrkdwn` with the status table in a code block

**Usage:**

```bash
python scripts/nightly_status/check_nightly_status.py [OPTIONS]
```

Generates `status.txt` (configurable via `STATUS_FILE` env var) containing the status summary table and failure details with AI-analyzed stacktraces, causes, and fixes.

| Option | Description |
|---|---|
| `--slack` | Output in Slack-formatted style with mrkdwn |
| `--cause` / `--no-cause` | Enable/disable AI cause analysis |
| `--fix` / `--no-fix` | Enable/disable AI fix suggestions |
| `--claude` / `--no-claude` | Use Claude CLI or NVIDIA LLM for analysis |
| `--print-logs` | Print full failed log tails |
| `--date YYYY-MM-DD` | Fetch status for a specific date (default: auto-detect) |

**Environment variables:**

| Variable | Description |
|---|---|
| `REPO` | Target repo (default: auto-detect via `gh`) |
| `ANTHROPIC_API_KEY` | API key for Claude CLI |
| `NVIDIA_API_KEY` / `LLM_API_KEY` | API key for NVIDIA LLM |
| `LLM_MODEL` | LLM model name |
| `LLM_API_URL` | LLM API endpoint |
| `STATUS_FILE` | Output file path (default: `status.txt`) |
| `GH_RETRIES` | Number of retries for GitHub API calls (default: 5) |
| `GH_HTTP_TIMEOUT` | HTTP timeout for GitHub API calls (default: 60) |

#### `search_upstream_issues.py`

Parses the status report, extracts error-specific search queries (test names, symbols, archive names), and searches upstream GitHub repos for related issues and PRs.

- **Presto failures** → searches `prestodb/presto` + `facebookincubator/velox`
- **Velox failures** → searches `facebookincubator/velox` only
- Filters results to the last 30 days only
- Outputs a Slack-formatted RCA report

**Usage:**

```bash
python scripts/nightly_status/search_upstream_issues.py -i status.txt -o rca-report.txt
```

| Option | Description |
|---|---|
| `-i`, `--input` | Path to status.txt (default: `status.txt`) |
| `-o`, `--output` | Path to write the RCA report (default: `rca-report.txt`) |
| `--days N` | Search issues/PRs created within the last N days (default: 30) |
| `--max-results N` | Max results per search query per repo (default: 5) |
| `--repos REPO [...]` | Additional repos to search |

#### `prepare_slack_payload.py`

Converts report files into Slack Block Kit JSON payloads suitable for `slackapi/slack-github-action` with `payload-file-path` and incoming webhooks.

- Splits on `---` section delimiters
- Separates code-fenced blocks (`` ``` ``) into standalone Slack blocks
- Chunks long content to stay within Slack's 3000-character block text limit

**Usage:**

```bash
python scripts/nightly_status/prepare_slack_payload.py --file status.txt --output status-payload.json
```

## Required GitHub Secrets

| Secret | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | Claude CLI authentication for AI analysis |
| `SLACK_WEBHOOK_NIGHTLY_STATUS_URL` | Incoming webhook URL for posting status and RCA reports to Slack |

## Manual Trigger

```bash
# Run with defaults
gh workflow run "Nightly Status Report" --ref nightly-status

# Run for a specific date
gh workflow run "Nightly Status Report" --ref nightly-status -f date=2026-03-12

# Skip upstream RCA search
gh workflow run "Nightly Status Report" --ref nightly-status -f skip_rca=true
```
