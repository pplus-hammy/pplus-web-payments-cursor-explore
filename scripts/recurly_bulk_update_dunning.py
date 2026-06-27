#!/usr/bin/env python3
"""Bulk-update Recurly account dunning_campaign_id from a CSV (v2021-02-25 API).

After each successful dunning update, adds an account note documenting the change.
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from zoneinfo import ZoneInfo

API_BASE = "https://v3.recurly.com"
DEFAULT_CREDS = Path("/Users/gregory.hamilton/Desktop/Creds/recurly_us_sbx.txt")
DEFAULT_CSV = Path("/Users/gregory.hamilton/Downloads/g_sbx_dunning_test.csv")
NY = ZoneInfo("America/New_York")
ACCEPT = "application/vnd.recurly.v2021-02-25"


def read_api_key(path: Path) -> str:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise SystemExit("API key file is empty: {}".format(path))
    return text.splitlines()[0].strip()


def ny_timestamp() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d %H:%M:%S")


def _account_id_for_api(account_cd: str) -> str:
    """Match recurly.Client / api/expire_subscriptions.py: use code- prefix for account codes."""
    value = (account_cd or "").strip()
    if not value:
        return value
    return value if value.startswith("code-") else "code-{}".format(value)


def account_put_url(account_cd: str) -> str:
    """
    PUT path must match the official recurly Python client: /accounts/{account_id} only.
    Site is implied by the API key (same as recurly.Client in expire_subscriptions.py).
    Do NOT use /sites/subdomain-.../accounts/... — that breaks single-site keys.
    """
    account_id = _account_id_for_api(account_cd)
    path = "/accounts/{}".format(quote(account_id, safe=""))
    return "{}{}".format(API_BASE, path)


def account_notes_post_url(account_cd: str) -> str:
    """POST /accounts/{account_id}/notes (create_account_note in recurly.Client)."""
    account_id = _account_id_for_api(account_cd)
    path = "/accounts/{}/notes".format(quote(account_id, safe=""))
    return "{}{}".format(API_BASE, path)


def note_message_for_dunning(dunning_campaign: str) -> str:
    return "Changed account to use {} as the dunning id".format(dunning_campaign)


def _csv_safe_detail(text: str, max_len: int = 800) -> str:
    """Single line, truncated; safe for CSV cells."""
    line = " ".join((text or "").split())
    if len(line) > max_len:
        return line[: max_len - 3] + "..."
    return line


def _format_recurly_error_body(http_status: int, body: str) -> str:
    """Build a short reason from Recurly JSON error body or raw text."""
    body = body.strip()
    if not body:
        return "HTTP {}".format(http_status)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return _csv_safe_detail("HTTP {} {}".format(http_status, body))

    err = data.get("error")
    if isinstance(err, dict):
        parts = []
        t = err.get("type")
        if t is not None:
            parts.append(str(t))
        msg = err.get("message")
        if msg is not None:
            parts.append(str(msg))
        for p in err.get("params") or ():
            if isinstance(p, dict):
                pm = p.get("message")
                pp = p.get("param")
                if pm is not None:
                    parts.append("{}: {}".format(pp or "param", pm))
        if parts:
            return _csv_safe_detail("HTTP {} {}".format(http_status, "; ".join(parts)))
    return _csv_safe_detail("HTTP {} {}".format(http_status, body))


def put_account(api_key: str, url: str, dunning_campaign_id: str) -> tuple[int, str | None, str]:
    """Return (http_status, error_detail, response_body). error_detail is None on 2xx."""
    body = json.dumps({"dunning_campaign_id": dunning_campaign_id}).encode("utf-8")
    req = Request(url, data=body, method="PUT")
    req.add_header("Accept", ACCEPT)
    req.add_header("Content-Type", "application/json")
    req.add_header("Idempotency-Key", str(uuid.uuid4()))
    basic = base64.b64encode("{}:".format(api_key).encode("utf-8")).decode("ascii").replace("\n", "")
    req.add_header("Authorization", "Basic {}".format(basic))
    try:
        with urlopen(req, timeout=120) as resp:
            code = int(resp.getcode())
            raw = resp.read().decode("utf-8", errors="replace")
            if 200 <= code < 300:
                return code, None, raw
            return code, _format_recurly_error_body(code, raw), raw
    except HTTPError as e:
        code = int(e.code)
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return code, _format_recurly_error_body(code, raw), raw


def post_account_note(api_key: str, url: str, message: str) -> tuple[int, str | None, str]:
    """POST account note; returns (http_status, error_detail, response_body) like put_account."""
    body = json.dumps({"message": message}).encode("utf-8")
    req = Request(url, data=body, method="POST")
    req.add_header("Accept", ACCEPT)
    req.add_header("Content-Type", "application/json")
    req.add_header("Idempotency-Key", str(uuid.uuid4()))
    basic = base64.b64encode("{}:".format(api_key).encode("utf-8")).decode("ascii").replace("\n", "")
    req.add_header("Authorization", "Basic {}".format(basic))
    try:
        with urlopen(req, timeout=120) as resp:
            code = int(resp.getcode())
            raw = resp.read().decode("utf-8", errors="replace")
            if 200 <= code < 300:
                return code, None, raw
            return code, _format_recurly_error_body(code, raw), raw
    except HTTPError as e:
        code = int(e.code)
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            raw = ""
        return code, _format_recurly_error_body(code, raw), raw


def print_first_api_response(
    http_status: int,
    raw_body: str,
    fallback_detail: str | None = None,
) -> None:
    """Print full API response to stdout (first CSV row that performs a PUT only)."""
    label = http_status if http_status else "n/a"
    print("--- First record: API response (HTTP {}) ---".format(label))
    text = (raw_body or "").strip()
    if text:
        try:
            print(json.dumps(json.loads(text), indent=2, ensure_ascii=False))
        except json.JSONDecodeError:
            print(text)
    elif fallback_detail:
        print("{}".format(fallback_detail))
    else:
        print("(empty body)")
    print("--- end first API response ---")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PUT dunning_campaign_id for each account_cd in a CSV via Recurly v3 API.",
    )
    p.add_argument(
        "--creds",
        type=Path,
        default=DEFAULT_CREDS,
        help="Path to file containing the API key (first line). Default: %(default)s",
    )
    p.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Input CSV with columns account_cd, dunning_campaign. Default: %(default)s",
    )
    p.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Output log CSV path. Default: <csv_dir>/<csv_stem>_recurly_dunning_log_<ny_ts>.csv",
    )
    p.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Optional pause between requests (rate limiting). Default: 0",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    creds_path = args.creds.resolve()
    csv_path = args.csv.resolve()

    if not creds_path.is_file():
        raise SystemExit("Creds file not found: {}".format(creds_path))
    if not csv_path.is_file():
        raise SystemExit("CSV file not found: {}".format(csv_path))

    api_key = read_api_key(creds_path)

    run_ts = datetime.now(NY).strftime("%Y-%m-%d_%H%M%S")
    log_path = args.log
    if log_path is None:
        log_path = csv_path.parent / "{}_recurly_dunning_log_{}.csv".format(
            csv_path.stem,
            run_ts,
        )
    else:
        log_path = log_path.resolve()

    skip_header = log_path.exists() and log_path.stat().st_size > 0

    # utf-8-sig strips UTF-8 BOM (\ufeff) from the first cell (common for Excel exports).
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit("CSV has no header row.")
        headers = [(h or "").strip().lstrip("\ufeff") for h in reader.fieldnames if h is not None]
        if "account_cd" not in headers or "dunning_campaign" not in headers:
            raise SystemExit(
                "CSV must include columns account_cd and dunning_campaign; got: {}".format(
                    reader.fieldnames,
                )
            )

        with log_path.open("a", encoding="utf-8", newline="") as logf:
            writer = csv.writer(logf)
            if not skip_header:
                writer.writerow(["account_cd", "status", "timestamp_ny", "detail"])

            printed_first_api = False
            for row in reader:
                account_cd = (row.get("account_cd") or "").strip()
                dunning_campaign = (row.get("dunning_campaign") or "").strip()

                if not account_cd and not dunning_campaign:
                    continue

                ts = ny_timestamp()
                if not account_cd:
                    writer.writerow(["", "failure", ts, "validation: missing account_cd"])
                    logf.flush()
                    continue
                if not dunning_campaign:
                    writer.writerow(
                        [account_cd, "failure", ts, "validation: missing dunning_campaign"],
                    )
                    logf.flush()
                    continue

                url = account_put_url(account_cd)
                ok = False
                last_status = 0
                last_detail: str | None = None
                last_raw_body = ""
                for attempt in range(3):
                    try:
                        status, err_detail, raw_body = put_account(api_key, url, dunning_campaign)
                        last_status = status
                        last_detail = err_detail
                        last_raw_body = raw_body
                        if status == 429 and attempt < 2:
                            time.sleep(2.0 * (attempt + 1))
                            continue
                        ok = 200 <= status < 300
                        break
                    except URLError as ue:
                        last_status = 0
                        last_detail = _csv_safe_detail(
                            "network_error: {}".format(ue.reason or type(ue).__name__),
                        )
                        last_raw_body = ""
                        if attempt < 2:
                            time.sleep(1.0 * (attempt + 1))
                            continue
                        ok = False
                        break

                if not printed_first_api:
                    print_first_api_response(
                        last_status,
                        last_raw_body,
                        fallback_detail=last_detail,
                    )
                    printed_first_api = True

                status_label = "success" if ok else "failure"
                detail_out = ""
                if not ok:
                    detail_out = last_detail or _csv_safe_detail(
                        "HTTP {} (no error detail)".format(last_status),
                    )
                else:
                    note_url = account_notes_post_url(account_cd)
                    note_text = note_message_for_dunning(dunning_campaign)
                    try:
                        n_status, n_detail, _n_raw = post_account_note(
                            api_key,
                            note_url,
                            note_text,
                        )
                        if not (200 <= n_status < 300):
                            detail_out = _csv_safe_detail(
                                "dunning updated; account note failed: {}".format(
                                    n_detail or "HTTP {}".format(n_status),
                                ),
                            )
                    except URLError as ue:
                        detail_out = _csv_safe_detail(
                            "dunning updated; account note failed: {}".format(
                                ue.reason or type(ue).__name__,
                            ),
                        )

                writer.writerow([account_cd, status_label, ts, detail_out])
                logf.flush()

                if args.delay_seconds > 0:
                    time.sleep(args.delay_seconds)

    print("Wrote log: {}".format(log_path), file=sys.stderr)


if __name__ == "__main__":
    main()
