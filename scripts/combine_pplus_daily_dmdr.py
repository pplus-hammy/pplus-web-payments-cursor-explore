#!/usr/bin/env python3
"""Combine Cybersource DMDR daily CSVs: drop row 1 per file, normalize headers (spaces → _)."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

DEFAULT_INPUT_DIR = Path("/Users/gregory.hamilton/Desktop/cybs_files/dmdr")
OUTPUT_PREFIX = "combined_pplus_daily_dmdr_"


def normalize_header_cell(value: str) -> str:
    return value.replace(" ", "_")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir",
        nargs="?",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing *.csv files (default: %(default)s)",
    )
    args = parser.parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        raise SystemExit("Not a directory: {}".format(input_dir))

    today_tag = datetime.now().strftime("%Y%m%d")
    output_path = input_dir / "{}{}.csv".format(OUTPUT_PREFIX, today_tag)

    paths = sorted(
        p
        for p in input_dir.glob("*.csv")
        if not p.name.startswith(OUTPUT_PREFIX)
    )
    if not paths:
        raise SystemExit("No CSV files found in {}".format(input_dir))

    merged: list[list[str]] = []
    header_out: list[str] | None = None

    for path in paths:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            continue
        rows = rows[1:]  # remove first row
        if not rows:
            continue

        raw_header = rows[0]
        data_rows = rows[1:]
        header_row = [normalize_header_cell(c) if isinstance(c, str) else c for c in raw_header]

        if header_out is None:
            header_out = header_row
            merged.append(header_row)
        merged.extend(data_rows)

    if header_out is None:
        raise SystemExit("No data after processing files in {}".format(input_dir))

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(merged)

    print("Wrote {} row(s) to {}".format(len(merged), output_path))


if __name__ == "__main__":
    main()
