#!/usr/bin/env python3
"""
Run queries/txn_dow_hourly_bin_stddev.sql every hour (invoke via cron at :10 past)
and email results to the Slack channel's email address.

Environment variables:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_FROM_EMAIL  (required for email)
  SMTP_PASSWORD  (required unless SMTP_PASSWORD_FILE is set)
  SMTP_PASSWORD_FILE  (optional: path to file containing password; used instead of SMTP_PASSWORD)
  BIGQUERY_PROJECT  (optional, default: i-dss-streaming-data)
  EMAIL_MAX_ROWS    (optional, default: 25 — max rows included in email body)

Cron example (run at 10 minutes past every hour):
  10 * * * * cd /path/to/pplus-web-payments-cursor-explore && .venv/bin/python scripts/run_dow_hourly_slack.py
  (Or: python scripts/run_dow_hourly_slack.py — script re-execs with .venv if not already in a venv.)

Set SMTP_* and optionally BIGQUERY_PROJECT in crontab or via a wrapper that sources .env.
BigQuery uses Application Default Credentials (run scripts/setup-adc.sh first).

When run without an active virtual environment, the script re-execs using the
project's .venv/bin/python so dependencies (e.g. google-cloud-bigquery) are
loaded from the project venv. Create the venv with:
  python -m venv .venv && .venv/bin/pip install -r requirements.txt
"""

from __future__ import annotations

import io
import os
import smtplib
import sys
from email.generator import Generator
from email.mime.text import MIMEText
from email.policy import SMTP
from pathlib import Path

# Run in project .venv if not already in a virtual environment
def _repo_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    return script_dir.parent if script_dir.name == "scripts" else script_dir


_root = _repo_root()
_venv_py = _root / ".venv" / "bin" / "python"
if _venv_py.exists() and sys.prefix == sys.base_prefix:
    os.execv(str(_venv_py), [str(_venv_py)] + sys.argv)

from dotenv import load_dotenv
import pandas as pd

from google.cloud import bigquery

load_dotenv(_root / ".env")

# Fixed recipient: Slack channel email (emails post to channel)
SLACK_CHANNEL_EMAIL = "d2c-payment-fraud-mon-aaaatanf5wtlalz6jddjekf3pe@paramountglobal.org.slack.com"


def repo_root() -> Path:
    """Resolve repo root so script finds queries/ whether run from project root or scripts/."""
    script_dir = Path(__file__).resolve().parent
    # If we're in scripts/, parent is repo root
    if script_dir.name == "scripts":
        return script_dir.parent
    return script_dir


def load_sql() -> str:
    path = repo_root() / "queries" / "txn_dow_hourly_bin_stddev_EMAIL.sql"
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text()


def run_query(project: str) -> list[dict]:
    """Execute the BigQuery script and return all rows from the last SELECT."""
    client = bigquery.Client(project=project)
    sql = load_sql()
    query_job = client.query(sql)
    rows = list(query_job.result())
    return [dict(row.items()) for row in rows]


def format_body_from_df(df: pd.DataFrame, max_rows: int) -> str:
    """Build HTML email body from DataFrame: summary + truncated table (pandas to_html)."""
    from datetime import datetime

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    n = len(df)
    if n == 0:
        return (
            "<html><body>"
            "<p><b>Hourly BIN anomaly check </b> — " + ts + "</p>"
            "<p>No anomalies.</p>"
            "</body></html>"
        )

    # Include all columns returned by the query
    show = df.head(max_rows)
    # User-friendly HTML table via pandas to_html (index=False for no row numbers)
    table_html = show.to_html(index=False, escape=True)
    row_cap = f" (first {min(max_rows, n)} of {n} rows)" if n > max_rows else ""
    return (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        "<style>"
        "table { border-collapse: collapse; border: 1px solid #ccc; width: 100%; font-size: 14px; }"
        "th, td { border: 1px solid #ccc; padding: 6px 8px; text-align: left; }"
        "th { background: #eee; }"
        "</style></head><body>"
        "<p><b>Hourly BIN anomaly check</b> — " + ts + "</p>"
        "<p>Rows: " + str(n) + row_cap + "</p>"
        "<div style=\"overflow-x: auto;\">" + table_html + "</div>"
        "</body></html>"
    )


def _ascii_safe(s: str) -> str:
    """Replace common non-ASCII chars with ASCII equivalents for SMTP-safe content."""
    return (
        s.replace("\xa0", " ")   # non-breaking space -> space
        .replace("\u2014", "-")  # em dash -> hyphen
        .replace("\u2013", "-")  # en dash -> hyphen
        .encode("ascii", "replace")
        .decode("ascii")
    )


def send_email(subject: str, body: str, *, html: bool = False) -> None:
    """Send email to the Slack channel address via SMTP (plain text or HTML)."""
    subject = _ascii_safe(subject)
    if not html:
        body = _ascii_safe(body)

    host = os.environ.get("SMTP_HOST")
    port = os.environ.get("SMTP_PORT")
    user = os.environ.get("SMTP_USER")
    from_addr = os.environ.get("SMTP_FROM_EMAIL")
    password_file = os.environ.get("SMTP_PASSWORD_FILE")
    if password_file and Path(password_file).is_file():
        password = Path(password_file).read_text().strip()
    else:
        password = os.environ.get("SMTP_PASSWORD")
    for var, name in [
        (host, "SMTP_HOST"),
        (port, "SMTP_PORT"),
        (user, "SMTP_USER"),
        (from_addr, "SMTP_FROM_EMAIL"),
        (password, "SMTP_PASSWORD or SMTP_PASSWORD_FILE"),
    ]:
        if not var:
            raise ValueError(f"Missing required env var: {name}")

    port = int(port)
    subtype = "html" if html else "plain"
    msg = MIMEText(body, subtype, "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = SLACK_CHANNEL_EMAIL

    # Serialize with SMTP policy (body encoded as quoted-printable or base64)
    buf = io.StringIO()
    Generator(buf, policy=SMTP).flatten(msg)
    msg_str = buf.getvalue()
    if not html:
        msg_str = msg_str.encode("ascii", "replace").decode("ascii")
    msg_bytes = msg_str.encode("utf-8")

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.sendmail(from_addr, [SLACK_CHANNEL_EMAIL], msg_bytes)


def main() -> int:
    from datetime import datetime

    project = os.environ.get("BIGQUERY_PROJECT", "i-dss-streaming-data")
    max_rows = int(os.environ.get("EMAIL_MAX_ROWS", "25"))

    try:
        print("Running query...", file=sys.stderr)
        rows = run_query(project)
        print(f"Query returned {len(rows)} rows.", file=sys.stderr)
    except Exception as e:
        print(f"BigQuery error: {e}", file=sys.stderr)
        try:
            send_email(
                f"DOW hourly run failed: {str(e)[:80]}",
                f"DOW hourly anomaly check failed at {datetime.now().isoformat()}\n\n{type(e).__name__}: {e}",
            )
        except Exception as send_err:
            print(f"Failed to send error email: {send_err}", file=sys.stderr)
        return 1

    df = pd.DataFrame(rows)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if len(df) == 0:
        subject = f"No anomalies — {ts}"
    else:
        subject = f"Hourly BIN Anomalies — {ts}"

    print("Drafting email...", file=sys.stderr)
    body = format_body_from_df(df, max_rows)
    try:
        send_email(subject, body, html=True)
        print("Email sent.", file=sys.stderr)
    except Exception as e:
        print(f"SMTP error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
