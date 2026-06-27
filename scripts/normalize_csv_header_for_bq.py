#!/usr/bin/env python3
"""Drop CSV line 1; use line 2 as headers (spaces → underscores, lowercase); save as *_bq.csv."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

# DEFAULT_INPUT = "/Users/gregory.hamilton/Downloads/dm_april_fraud.csv"
DEFAULT_INPUT = '/Users/gregory.hamilton/Desktop/cybs_files/dmdr/dmdr_20260615.csv'


def _normalize_header(name: str) -> str:
    return name.strip().replace(" ", "_").lower()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Skip row 1; normalize row 2 headers; write <stem>_bq.csv next to input.",
    )
    parser.add_argument(
        "input_path",
        nargs="?",
        default=DEFAULT_INPUT,
        type=Path,
        help=f"Source CSV (default: {DEFAULT_INPUT})",
    )
    args = parser.parse_args()
    path: Path = args.input_path

    if not path.is_file():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)

    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        rows = list(csv.reader(f))

    if len(rows) < 2:
        print("Error: need at least two rows (junk row + header row).", file=sys.stderr)
        sys.exit(1)

    header = [_normalize_header(h) for h in rows[1]]
    data = rows[2:]
    out_path = path.parent / f"{path.stem}_bq{path.suffix}"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(data)

    print(out_path)


if __name__ == "__main__":
    main()
