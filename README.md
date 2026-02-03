# pplus-web-payments-cursor-explore

BigQuery queries and scripts for payment/transaction analysis.

## Hourly DOW anomaly check → Slack (email)

The script `scripts/run_dow_hourly_slack.py` runs `queries/txn_dow_hourly_bin_stddev.sql` and emails a formatted summary to a Slack channel. Schedule it at **10 minutes past every hour** via cron.

### Requirements

- **BigQuery**: Application Default Credentials (run `./scripts/setup-adc.sh` once).
- **SMTP**: Credentials and a "From" address (e.g. Google Workspace SMTP relay, SendGrid, or corporate SMTP).

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SMTP_HOST` | Yes | SMTP server hostname |
| `SMTP_PORT` | Yes | SMTP port (e.g. 587) |
| `SMTP_USER` | Yes | SMTP username |
| `SMTP_PASSWORD` | Yes | SMTP password (or app password) |
| `SMTP_FROM_EMAIL` | Yes | Sender address for the report email |
| `BIGQUERY_PROJECT` | No | BigQuery project (default: `i-dss-streaming-data`) |
| `EMAIL_MAX_ROWS` | No | Max rows in email body (default: 25) |

Set these in your shell, in crontab, or in a `.env` file (do not commit `.env`).

### Virtual environment

Requires **Python 3.13** (see `.python-version`). Create and use the project venv so the script has its dependencies:

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If you run the script without an active venv (e.g. `python scripts/run_dow_hourly_slack.py`), it will re-exec itself using `.venv/bin/python` so it always runs with the project environment.

### Cron example

Run at 10 minutes past every hour:

```bash
10 * * * * cd /path/to/pplus-web-payments-cursor-explore && .venv/bin/python scripts/run_dow_hourly_slack.py
```

You can also use `python scripts/run_dow_hourly_slack.py`; the script will switch to the project venv automatically.

Ensure the cron environment has access to Application Default Credentials and the SMTP env vars (e.g. source a script that exports them, or set them in the crontab line).

### Recipient

Results are sent to the Slack channel email address configured in the script (no webhook or Slack app setup required).
