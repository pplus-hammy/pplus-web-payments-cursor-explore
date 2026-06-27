#!/usr/bin/env python3
"""Combine Adyen fraud risk CSV exports into adyen_fraud_risk.csv."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

DEFAULT_INPUT_DIR = Path("/Users/gregory.hamilton/Desktop/adyen_ml_model/fraud_risk")
OUTPUT_NAME = "adyen_fraud_risk.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_dir",
        nargs="?",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing *.csv files (default: %(default)s)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_NAME,
        help="Output filename (default: %(default)s, written inside input_dir)",
    )
    args = parser.parse_args()
    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.is_dir():
        raise SystemExit("Not a directory: {}".format(input_dir))

    output_path = input_dir / args.output

    paths = sorted(
        p
        for p in input_dir.glob("*.csv")
        if p.name != output_path.name
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

        raw_header = rows[0]
        data_rows = rows[1:]
        if not data_rows:
            continue

        if header_out is None:
            header_out = raw_header
            merged.append(raw_header)
        merged.extend(data_rows)

    if header_out is None:
        raise SystemExit("No data after processing files in {}".format(input_dir))

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(merged)

    print(
        "Combined {} file(s), wrote {} row(s) to {}".format(
            len(paths), len(merged), output_path
        )
    )


if __name__ == "__main__":
    main()
