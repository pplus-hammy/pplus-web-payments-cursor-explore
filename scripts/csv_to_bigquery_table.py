#!/usr/bin/env python3
"""
Load a local CSV into BigQuery with every column stored as STRING.

This avoids autodetect failures when a CSV column mixes numbers, text, and odd
tokens in different rows. Parse or cast in SQL later (e.g. SAFE_CAST).

Edit CSV_PATH, BQ_DATASET, and BQ_TABLE below (and BQ_PROJECT or `.env`).

Optional junk row: set SKIP_JUNK_ROW True if line 1 is junk and line 2 is the
header (same behavior as scripts/load_csv_to_bigquery.py).

The CSV sent to BigQuery includes a header line only when the destination table
does not exist yet; that line is skipped in the load job so it is never inserted
as a row. If the table already exists, the upload has no header line.

BigQuery uses Application Default Credentials:

  gcloud auth application-default login

For automated checks without editing this file, you can point at a test CSV:

  CSV_TO_BQ_CSV_PATH=/tmp/mixed.csv .venv/bin/python scripts/csv_to_bigquery_table.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import time
from datetime import datetime
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
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

load_dotenv(_root / ".env")

# --- edit these for each load ---
# CSV_PATH = Path.home() / "Downloads" / "dm_april_fraud_bq.csv"
CSV_PATH = '/Users/gregory.hamilton/Desktop/cybs_files/dmdr/dmdr_202606_bq.csv'
BQ_PROJECT = os.environ.get("BQ_PROJECT", "i-dss-streaming-data")
BQ_DATASET = "payment_ops_sandbox"
BQ_TABLE = "cybs_dmdr_latam"
SKIP_JUNK_ROW = False
NORMALIZE_HEADERS = True
APPEND = True
# --- end edit ---


def _normalize_header(name: str) -> str:
    """
    Match repo CSV helpers (spaces → underscores, lowercase), then make a
    BigQuery-safe unquoted column name (letters, digits, underscore only).
    """
    s = name.strip().replace(" ", "_").lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "field"
    if s[0].isdigit():
        return f"c_{s}"
    return s


def _format_minutes_seconds(elapsed_sec: float) -> str:
    """Human-readable duration, e.g. 3m 15.2s or 0m 4.1s."""
    minutes = int(elapsed_sec // 60)
    seconds = elapsed_sec - minutes * 60
    return f"{minutes}m {seconds:.1f}s"


def _dedupe_column_names(names: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        cand = n
        if cand not in seen:
            seen.add(cand)
            out.append(cand)
            continue
        i = 2
        while True:
            cand = f"{n}_{i}"
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
                break
            i += 1
    return out


def _read_csv_as_strings(path: Path, *, skip_junk_row: bool, normalize_headers: bool) -> pd.DataFrame:
    """Read CSV without type inference; every column stays string."""
    read_kw: dict = {
        "dtype": str,
        "na_filter": False,
        "keep_default_na": False,
    }
    if skip_junk_row:
        raw_csv = path.read_bytes()
        text = raw_csv.decode("utf-8", errors="replace")
        lines = text.splitlines()
        if len(lines) < 2:
            raise ValueError("CSV has fewer than 2 lines (need junk row + header).")
        header_row = lines[1]
        reader = csv.reader(io.StringIO(header_row))
        raw_headers = next(reader)
        normalized_headers = _dedupe_column_names([_normalize_header(h) for h in raw_headers])
        buf = io.StringIO()
        buf.write(",".join(normalized_headers) + "\n")
        buf.write("\n".join(lines[2:]))
        buf.seek(0)
        return pd.read_csv(buf, **read_kw)

    df = pd.read_csv(
        path,
        encoding="utf-8",
        encoding_errors="replace",
        **read_kw,
    )
    if normalize_headers:
        df.columns = _dedupe_column_names([_normalize_header(str(c)) for c in df.columns])
    return df


def load_dataframe_all_string(
    df: pd.DataFrame,
    *,
    project: str,
    dataset: str,
    table: str,
    append: bool,
) -> int | None:
    """
    Load DataFrame to BigQuery. Returns row count reported by the completed load
    job (output_rows), or None if the API did not populate it.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    try:
        client.get_table(table_ref)
        table_exists = True
    except NotFound:
        table_exists = False

    schema = [bigquery.SchemaField(name, "STRING") for name in df.columns]
    buf = io.BytesIO()
    csv_kw = {"index": False, "date_format": "%Y-%m-%d %H:%M:%S"}
    if table_exists:
        df.to_csv(buf, header=False, **csv_kw)
        skip_leading_rows = 0
    else:
        df.to_csv(buf, header=True, **csv_kw)
        skip_leading_rows = 1
    buf.seek(0)
    write_disp = (
        bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False,
        write_disposition=write_disp,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=skip_leading_rows,
    )
    load_job = client.load_table_from_file(buf, table_ref, job_config=job_config)
    load_job.result()
    return load_job.output_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CSV to BigQuery (all STRING columns).")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read CSV and print shape/columns only; do not load to BigQuery.",
    )
    args = parser.parse_args()

    path = Path(os.environ["CSV_TO_BQ_CSV_PATH"]) if os.environ.get("CSV_TO_BQ_CSV_PATH") else Path(CSV_PATH)
    if not path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    try:
        df = _read_csv_as_strings(
            path,
            skip_junk_row=SKIP_JUNK_ROW,
            normalize_headers=NORMALIZE_HEADERS,
        )
    except Exception as e:
        print(f"Error reading CSV: {e}", file=sys.stderr)
        sys.exit(1)

    if df.empty:
        print("No data rows after header; nothing to load.", file=sys.stderr)
        sys.exit(0)

    if args.dry_run:
        print(f"Rows (data): {len(df)}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"Destination would be: {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
        return

    csv_rows = len(df)
    print(f"CSV data rows (excluding header): {csv_rows}")

    load_t0 = time.perf_counter()
    load_started_at = datetime.now()
    print(
        f"BigQuery load starting at {load_started_at.isoformat(timespec='seconds')} "
        f"— file: {path.name} ({path.resolve()})"
    )

    try:
        bq_rows = load_dataframe_all_string(
            df,
            project=BQ_PROJECT,
            dataset=BQ_DATASET,
            table=BQ_TABLE,
            append=APPEND,
        )
    except Exception as e:
        elapsed = time.perf_counter() - load_t0
        load_ended_at = datetime.now()
        print(
            f"BigQuery load ended at {load_ended_at.isoformat(timespec='seconds')} "
            f"— duration: {_format_minutes_seconds(elapsed)} (load failed)",
            file=sys.stderr,
        )
        print(f"BigQuery error: {e}", file=sys.stderr)
        sys.exit(1)

    elapsed = time.perf_counter() - load_t0
    load_ended_at = datetime.now()
    print(
        f"BigQuery load ended at {load_ended_at.isoformat(timespec='seconds')} "
        f"— duration: {_format_minutes_seconds(elapsed)}"
    )

    action = "Appended" if APPEND else "Loaded"
    print(f"{action} into {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    if bq_rows is not None:
        print(f"BigQuery load job reported rows: {bq_rows}")
    else:
        print(
            "BigQuery load job did not return output_rows; compare row counts in the console or query the table.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
