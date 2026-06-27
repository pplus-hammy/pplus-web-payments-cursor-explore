#!/usr/bin/env python3
"""
Load a local CSV into BigQuery after skipping the first row and normalizing headers.

The first line of the file is ignored. The second line is treated as column names:
spaces are replaced with underscores and names are lowercased (same rules as
`scripts/download_cybersource_daily_report.py`).

BigQuery uses Application Default Credentials. From a machine with gcloud:

  gcloud auth application-default login

Example (replace DATASET and TABLE with your destination):

  .venv/bin/python scripts/load_csv_to_bigquery.py \\
    --dataset payment_ops_sandbox \\
    --table dm_april_fraud

Default CSV path: /Users/gregory.hamilton/Downloads/dm_april_fraud.csv

Optional: set defaults in `.env`, e.g. BQ_PROJECT=i-dss-streaming-data
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
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


DEFAULT_CSV_PATH = "/Users/gregory.hamilton/Downloads/dm_april_fraud.csv"
DEFAULT_PROJECT = os.environ.get("BQ_PROJECT", "i-dss-streaming-data")


def _normalize_header(name: str) -> str:
    """Replace spaces with underscores and convert to lowercase."""
    return name.strip().replace(" ", "_").lower()


def process_csv(raw_csv: bytes) -> pd.DataFrame:
    """
    Skip first row; use row 2 as header; normalize header names
    (spaces → underscores, lowercase).
    """
    text = raw_csv.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if len(lines) < 2:
        raise ValueError("CSV has fewer than 2 lines (need at least junk row + header).")
    header_row = lines[1]
    reader = csv.reader(io.StringIO(header_row))
    raw_headers = next(reader)
    normalized_headers = [_normalize_header(h) for h in raw_headers]
    buf = io.StringIO()
    buf.write(",".join(normalized_headers) + "\n")
    buf.write("\n".join(lines[2:]))
    buf.seek(0)
    return pd.read_csv(buf)


def load_dataframe_to_bigquery(
    df: pd.DataFrame,
    *,
    project: str,
    dataset: str,
    table: str,
    append: bool,
) -> None:
    """Write DataFrame to BigQuery via in-memory CSV (no pyarrow)."""
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    buf = io.BytesIO()
    df.to_csv(buf, index=False, date_format="%Y-%m-%d %H:%M:%S")
    buf.seek(0)
    write_disp = (
        bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disp,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=0,
    )
    load_job = client.load_table_from_file(buf, table_ref, job_config=job_config)
    load_job.result()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load CSV to BigQuery (skip line 1; normalize line-2 headers).",
    )
    parser.add_argument(
        "--csv-path",
        default=DEFAULT_CSV_PATH,
        help=f"Input CSV file (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        metavar="PROJECT",
        help=f"GCP project id (default: {DEFAULT_PROJECT} or BQ_PROJECT from .env)",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        metavar="DATASET",
        help="BigQuery dataset id",
    )
    parser.add_argument(
        "--table",
        required=True,
        metavar="TABLE",
        help="BigQuery table id",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the table instead of replacing (WRITE_APPEND)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row and column info only; do not load to BigQuery.",
    )
    args = parser.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    raw = path.read_bytes()
    df = process_csv(raw)
    if df.empty:
        print("No data rows after header; nothing to load.", file=sys.stderr)
        sys.exit(0)

    if args.dry_run:
        print(f"Rows (data): {len(df)}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        return

    try:
        load_dataframe_to_bigquery(
            df,
            project=args.project,
            dataset=args.dataset,
            table=args.table,
            append=args.append,
        )
        action = "Appended" if args.append else "Loaded"
        print(f"{action} {len(df)} rows into {args.project}.{args.dataset}.{args.table}")
    except Exception as e:
        print(f"BigQuery error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
