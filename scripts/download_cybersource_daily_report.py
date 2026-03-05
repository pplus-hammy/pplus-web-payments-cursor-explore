#!/usr/bin/env python3
"""
Download the CyberSource daily_dmdr_dom report and upsert into BigQuery.

Downloads from CyberSource Reporting API (production), skips row 1 (row 2 = headers),
normalizes headers (spaces → underscores, lowercase), and MERGEs into
i-dss-streaming-data.payment_ops_sandbox.d2c_cybs_dmdr on merchant_id and request_id.

Environment variables (placeholders; set before running):
  CYBERSOURCE_KEY_ID       – API key / serial number (placeholder: YOUR_KEY_ID)
  CYBERSOURCE_SHARED_SECRET – Shared secret (placeholder: YOUR_SHARED_SECRET)

BigQuery uses Application Default Credentials (run gcloud auth application-default login
or scripts/setup-adc.sh once).

Cron example (daily at 8:00 AM local time). Replace REPO_PATH with your repo path and
set CYBERSOURCE_KEY_ID and CYBERSOURCE_SHARED_SECRET in crontab or via a wrapper that
sources .env:

  0 8 * * * cd REPO_PATH && .venv/bin/python scripts/download_cybersource_daily_report.py >> /tmp/cybersource_dmdr_cron.log 2>&1

With env in crontab:
  CYBERSOURCE_KEY_ID=your_key_id
  CYBERSOURCE_SHARED_SECRET=your_secret
  0 8 * * * cd REPO_PATH && .venv/bin/python scripts/download_cybersource_daily_report.py >> /tmp/cybersource_dmdr_cron.log 2>&1
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import io
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse

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
import requests

from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions

load_dotenv(_root / ".env")

# --- Config (fixed per plan) ---
ORGANIZATION_ID = "cbsinteractive_acct"
BASE_URL = "https://api.cybersource.com"
REPORT_NAME = "daily_dmdr_dom"
BQ_PROJECT = "i-dss-streaming-data"
BQ_DATASET = "payment_ops_sandbox"
BQ_TABLE = "d2c_cybs_dmdr"
BQ_STAGING_TABLE = "d2c_cybs_dmdr_staging"
MERGE_KEYS = ("merchant_id", "request_id")

# Placeholders: read from env; user must set real values
CYBERSOURCE_KEY_ID = os.environ.get("CYBERSOURCE_KEY_ID", "YOUR_KEY_ID")
CYBERSOURCE_SHARED_SECRET = os.environ.get("CYBERSOURCE_SHARED_SECRET", "YOUR_SHARED_SECRET")


def _normalize_header(name: str) -> str:
    """Replace spaces with underscores and convert to lowercase."""
    return name.strip().replace(" ", "_").lower()


def _digest_empty_body() -> str:
    """Digest for GET (no body): SHA-256 of empty string, base64."""
    h = hashlib.sha256(b"").digest()
    return "SHA-256=" + base64.b64encode(h).decode("ascii")


def _build_signature(
    method: str,
    resource_path: str,
    host: str,
    date_str: str,
    digest_val: str,
    merchant_id: str,
    key_id: str,
    shared_secret: str,
) -> str:
    """Build HTTP Signature header value (HmacSHA256) per CyberSource."""
    request_target = f"{method.lower()} {resource_path}"
    signing_string = (
        f"host: {host}\n"
        f"date: {date_str}\n"
        f"(request-target): {request_target}\n"
        f"digest: {digest_val}\n"
        f"v-c-merchant-id: {merchant_id}"
    )
    secret_bytes = base64.b64decode(shared_secret)
    sig_bytes = hmac.new(secret_bytes, signing_string.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = base64.b64encode(sig_bytes).decode("ascii")
    return (
        f'keyid="{key_id}", algorithm="HmacSHA256", '
        f'headers="host date (request-target) digest v-c-merchant-id", signature="{sig_b64}"'
    )


def download_report(report_date: str) -> bytes:
    """
    GET report from CyberSource Reporting API (production).
    report_date: YYYYMMDD.
    Returns raw CSV bytes.
    """
    if CYBERSOURCE_KEY_ID == "YOUR_KEY_ID" or CYBERSOURCE_SHARED_SECRET == "YOUR_SHARED_SECRET":
        raise ValueError(
            "Set CYBERSOURCE_KEY_ID and CYBERSOURCE_SHARED_SECRET (env or .env). "
            "Do not use placeholder values in production."
        )

    parsed = urlparse(BASE_URL)
    host = parsed.netloc or parsed.path
    path = "/reporting/v3/report-downloads"
    params = {
        "organizationId": ORGANIZATION_ID,
        "reportDate": report_date,
        "reportName": REPORT_NAME,
    }
    resource_path = path + "?" + urlencode(params)
    url = BASE_URL + resource_path

    # Date in RFC 7231 format (GMT)
    date_str = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    digest_val = _digest_empty_body()
    signature = _build_signature(
        "GET",
        resource_path,
        host,
        date_str,
        digest_val,
        ORGANIZATION_ID,
        CYBERSOURCE_KEY_ID,
        CYBERSOURCE_SHARED_SECRET,
    )

    headers = {
        "Host": host,
        "Date": date_str,
        "Digest": digest_val,
        "v-c-merchant-id": ORGANIZATION_ID,
        "Signature": signature,
        "Accept": "text/csv",
    }

    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code == 404:
        raise FileNotFoundError(
            f"Report not found (404) for reportDate={report_date}, reportName={REPORT_NAME}. "
            "Report may not be generated yet."
        )
    if resp.status_code == 400:
        raise ValueError(f"Bad request (400): {resp.text[:500]}")
    resp.raise_for_status()
    return resp.content


def process_csv(raw_csv: bytes) -> pd.DataFrame:
    """
    Skip first row; use row 2 as header; normalize header names
    (spaces → underscores, lowercase).
    """
    text = raw_csv.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) < 2:
        raise ValueError("CSV has fewer than 2 lines (need at least junk row + header).")
    # Row 0 = junk, row 1 = header
    header_row = lines[1]
    data_lines = lines[2:]
    reader = csv.reader(io.StringIO(header_row))
    raw_headers = next(reader)
    normalized_headers = [_normalize_header(h) for h in raw_headers]
    buf = io.StringIO()
    buf.write(",".join(normalized_headers) + "\n")
    buf.write("\n".join(lines[2:]))
    buf.seek(0)
    return pd.read_csv(buf)


def load_staging_and_merge(df: pd.DataFrame) -> None:
    """Load DataFrame to staging table, then MERGE into target on merchant_id and request_id."""
    client = bigquery.Client(project=BQ_PROJECT)
    staging_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_STAGING_TABLE}"
    target_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    # Use CSV load to avoid pyarrow dependency
    buf = io.BytesIO()
    df.to_csv(buf, index=False, date_format="%Y-%m-%d %H:%M:%S")
    buf.seek(0)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,
    )
    load_job = client.load_table_from_file(buf, staging_ref, job_config=job_config)
    load_job.result()

    # If target table does not exist, create it from staging (first run)
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    target_table_ref = dataset_ref.table(BQ_TABLE)
    try:
        client.get_table(target_table_ref)
    except google_exceptions.NotFound:
        create_sql = f"CREATE TABLE `{target_ref}` AS SELECT * FROM `{staging_ref}` LIMIT 0"
        client.query(create_sql).result()
        # Now append staging into empty target
        client.query(f"INSERT `{target_ref}` SELECT * FROM `{staging_ref}`").result()
        return

    # Build MERGE: update all columns on match, insert on no match
    columns = list(df.columns)
    set_clause = ", ".join(f"target.{c} = staging.{c}" for c in columns)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"staging.{c}" for c in columns)
    on_clause = " AND ".join(f"target.{k} = staging.{k}" for k in MERGE_KEYS)

    merge_sql = f"""
    MERGE `{target_ref}` AS target
    USING `{staging_ref}` AS staging
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    client.query(merge_sql).result()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download CyberSource daily_dmdr_dom report and upsert into BigQuery."
    )
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    parser.add_argument(
        "--report-date",
        default=yesterday,
        metavar="YYYYMMDD",
        help=f"Report date (default: yesterday={yesterday})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only download and parse CSV; do not load to BigQuery.",
    )
    parser.add_argument(
        "--load-only",
        metavar="FILE",
        help="Skip download; load from local CSV file and upsert to BigQuery.",
    )
    args = parser.parse_args()

    if args.load_only:
        path = Path(args.load_only)
        if not path.exists():
            print(f"Error: file not found: {path}", file=sys.stderr)
            sys.exit(1)
        raw = path.read_bytes()
    else:
        try:
            raw = download_report(args.report_date)
        except (ValueError, FileNotFoundError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    df = process_csv(raw)
    if df.empty:
        print("No data rows after header; nothing to load.", file=sys.stderr)
        sys.exit(0)

    if args.dry_run:
        print(f"Dry run: would load {len(df)} rows. Columns: {list(df.columns)}")
        return

    try:
        load_staging_and_merge(df)
        print(f"Upserted {len(df)} rows into {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    except Exception as e:
        print(f"BigQuery error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
