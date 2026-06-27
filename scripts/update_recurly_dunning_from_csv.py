#!/usr/bin/env python3
"""Bulk-update Recurly account dunning_campaign_id from a CSV (v2021-02-25 API).

CSV columns: account_cd, dunning_campaign (Recurly campaign code).
"""

import argparse
import csv
import sys
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# Recurly v3 host; sandbox site (cbscom-sand) is scoped by the API key.
API_BASE = "https://v3.recurly.com"
ACCEPT = "application/vnd.recurly.v2021-02-25"
NY = ZoneInfo("America/New_York")
DEFAULT_CREDS = Path("/Users/gregory.hamilton/Desktop/Creds/recurly_us_sbx.txt")
DEFAULT_CSV = Path("/Users/gregory.hamilton/Downloads/g_sbx_dunning_test.csv")


def make_session(api_key: str) -> requests.Session:
    """Authenticated session: API key as Basic-auth username, API version in Accept header."""
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers["Accept"] = ACCEPT
    return session


def fetch_campaign_map(session: requests.Session) -> dict[str, str]:
    """List all dunning campaigns and return a lookup of campaign code -> Recurly id."""
    mapping: dict[str, str] = {}
    url = f"{API_BASE}/dunning_campaigns"
    while url:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        for campaign in body.get("data", []):
            if campaign.get("code") and campaign.get("id"):
                mapping[campaign["code"]] = campaign["id"]
        url = body.get("next") or ""  # follow pagination until no more pages
    return mapping


def main() -> None:
    # --- CLI: creds file, input CSV, optional log path ---
    parser = argparse.ArgumentParser()
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--log", type=Path, default=None)
    args = parser.parse_args()

    # --- Setup: API session and campaign code -> id map ---
    api_key = args.creds.read_text(encoding="utf-8").strip().splitlines()[0].strip()
    session = make_session(api_key)
    campaigns = fetch_campaign_map(session)

    # Default log filename: <csv_stem>_recurly_dunning_log_<timestamp>.csv next to input CSV
    run_ts = datetime.now(NY).strftime("%Y-%m-%d_%H%M%S")
    log_path = args.log or args.csv.parent / f"{args.csv.stem}_recurly_dunning_log_{run_ts}.csv"

    # --- Process each CSV row: resolve campaign, PUT account update, log result ---
    with args.csv.open(encoding="utf-8-sig", newline="") as infile, log_path.open(
        "w", encoding="utf-8", newline=""
    ) as logfile:
        reader = csv.DictReader(infile)
        writer = csv.writer(logfile)
        writer.writerow(["account_cd", "status", "timestamp", "detail"])

        for row in reader:
            account_cd = (row.get("account_cd") or "").strip()
            campaign_code = (row.get("dunning_campaign") or "").strip()
            ts = datetime.now(NY).strftime("%Y-%m-%d %H:%M:%S")

            if not account_cd and not campaign_code:
                continue
            if not account_cd or not campaign_code:
                writer.writerow([account_cd, "failure", ts, "missing account_cd or dunning_campaign"])
                logfile.flush()
                continue

            campaign_id = campaigns.get(campaign_code)
            if not campaign_id:
                writer.writerow(
                    [account_cd, "failure", ts, f"unknown dunning_campaign_code: {campaign_code}"],
                )
                logfile.flush()
                continue

            # Recurly expects account path id as code-{account_cd}
            account_id = account_cd if account_cd.startswith("code-") else f"code-{account_cd}"
            resp = session.put(
                f"{API_BASE}/accounts/{account_id}",
                json={"dunning_campaign_id": campaign_id},
                headers={"Idempotency-Key": str(uuid.uuid4())},
                timeout=120,
            )
            ok = resp.ok
            detail = "" if ok else f"HTTP {resp.status_code} {resp.text[:300]}"
            writer.writerow([account_cd, "success" if ok else "failure", ts, detail])
            logfile.flush()

    print(f"Wrote log: {log_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
