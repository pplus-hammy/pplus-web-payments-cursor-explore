#!/usr/bin/env python3
"""Create a sample CSV with only the first N rows of an input CSV."""

import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Save first N rows of a CSV as a _sample file.")
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        default=Path("/Users/gregory.hamilton/Desktop/cybs_files/transaction/202602_g_all_bq.csv"),
        help="Input CSV path (default: 202602_g_all_bq.csv in cybs transaction folder)",
    )
    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to keep (default: 1000)",
    )
    parser.add_argument(
        "-s",
        "--start-row",
        type=int,
        default=791607857,
        help="1-based row number to start from (default: 791607857)",
    )
    args = parser.parse_args()

    input_path = args.input_path.resolve()
    if not input_path.exists():
        raise SystemExit("Input file not found: {}".format(input_path))

    # Output path: same dir, stem + _sample + suffix (e.g. 202602_g_all_bq_sample.csv)
    output_path = input_path.parent / "{}_sample{}".format(input_path.stem, input_path.suffix)

    skip_count = max(0, args.start_row - 1)

    with open(input_path, "r", newline="", encoding="utf-8", errors="replace") as fin:
        reader = csv.reader(fin)
        header = next(reader)
        rows = [header]
        for i, row in enumerate(reader):
            if i < skip_count:
                continue
            if len(rows) - 1 >= args.rows:
                break
            rows.append(row)

    with open(output_path, "w", newline="", encoding="utf-8") as fout:
        csv.writer(fout).writerows(rows)

    print("Wrote {} rows to {}".format(len(rows) - 1, output_path))


if __name__ == "__main__":
    main()
