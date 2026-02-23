#!/usr/bin/env python3
"""
Recurly subscription cleanup: expire subscriptions, add account notes,
clear billing info, and optionally refund invoices per CSV input.

Target: https://cbscom-sand.recurly.com/
API key: read from RECURLY_KEY_FILE or RECURLY_PRIVATE_API_KEY env.
Subscription list: CSV at DEFAULT_INPUT_CSV (expire_refund_script_test.csv) unless --input is set.

Authentication: Recurly expects the API key to be passed as the username in a
Basic Authorization header (password empty). The official recurly-client-python
library handles this when you pass the raw API key to recurly.Client(api_key).
Do not pre-encode the key; the client sends it as Basic base64(api_key + ":").
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

RECURLY_KEY_FILE = "/Users/gregory.hamilton/Desktop/Creds/recurly_us_sbx.txt"
DEFAULT_INPUT_CSV = "/Users/gregory.hamilton/Downloads/expire_refund_script_test.csv"
DEFAULT_LOG_DIR = "/Users/gregory.hamilton/Downloads"


def _default_log_path() -> str:
    """Default log path: expire_refund_script_log_yyyymmdd_hhmmss.csv in DEFAULT_LOG_DIR."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(Path(DEFAULT_LOG_DIR) / f"expire_refund_script_log_{ts}.csv")


def _load_api_key() -> str:
    """
    Load API key from RECURLY_KEY_FILE, then RECURLY_PRIVATE_API_KEY env.
    Raw key only; Recurly client sends it as Basic auth username.
    """
    path = Path(RECURLY_KEY_FILE)
    if path.exists():
        key = path.read_text(encoding="utf-8").strip()
        if key:
            return key
    return os.environ.get("RECURLY_PRIVATE_API_KEY", "YOUR_API_KEY")


RECURLY_API_KEY = _load_api_key()

try:
    import recurly
except ImportError:
    recurly = None


def _refund_invoice_body() -> Dict[str, Any]:
    """
    Build request body for refund_invoice (full refund).
    Per Recurly API v2021-02-25: use type "percentage" with percentage 100 for a full refund.
    https://recurly.com/developers/api/v2021-02-25/index.html#operation/refund_invoice
    """
    return {"type": "percentage", "percentage": 100}


def load_input(csv_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Load CSV with columns account_cd, subscription_guid, refund_dt, account_note (optional: currency_cd)."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    # utf-8-sig strips BOM (e.g. from Excel) so first column is "account_cd" not "\ufeffaccount_cd"
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return []
    # Normalize header keys: strip BOM and whitespace so columns are found
    for row in rows:
        for k in list(row.keys()):
            clean_key = k.strip().lstrip("\ufeff")
            if clean_key != k:
                row[clean_key] = row.pop(k)
    for col in ("account_cd", "subscription_guid", "refund_dt", "account_note"):
        if col not in rows[0]:
            raise ValueError(f"CSV must have column: {col}")
    # Normalize: treat None as empty string
    for row in rows:
        for k in list(row.keys()):
            v = row[k]
            row[k] = (str(v).strip() if v is not None and v != "" else "")
    return rows


def _subscription_id(value: str) -> str:
    """Return subscription_id for API: add uuid- prefix if value looks like a UUID."""
    s = (value or "").strip()
    if not s:
        return s
    # UUID-like: 8-4-4-4-12 hex
    clean = s.replace("-", "")
    if len(clean) == 32 and all(c in "0123456789abcdefABCDEF" for c in clean):
        return f"uuid-{s}" if not s.startswith("uuid-") else s
    return s


def _account_id(account_cd: str) -> str:
    """Return account_id for API: code- prefix."""
    value = (account_cd or "").strip()
    if not value:
        return value
    return value if value.startswith("code-") else f"code-{value}"


def _parse_refund_date(s: Optional[str]) -> Optional[date]:
    """
    Parse refund date from m/d/yyyy or yyyy-mm-dd. Returns date or None if invalid.
    """
    if not s or not (s := s.strip()):
        return None
    # yyyy-mm-dd (e.g. 2026-01-01)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            pass
    # mm/dd/yyyy (e.g. 01/01/2026)
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        pass
    # m/d/yyyy or m/d/yy (e.g. 1/1/2026, 1/1/26) via manual parse
    parts = s.replace(",", "").split("/")
    if len(parts) == 3:
        try:
            m, d, y = int(parts[0].strip()), int(parts[1].strip()), int(parts[2].strip())
            if y < 100:
                y += 2000 if y < 50 else 1900
            return date(y, m, d)
        except (ValueError, TypeError):
            pass
    return None


def _account_has_note_today(client: Any, acc_id: str, run_date: date) -> bool:
    """Return True if the account already has at least one note created on run_date."""
    try:
        pager = client.list_account_notes(acc_id)
        for note in pager.items():
            created = getattr(note, "created_at", None)
            if not created:
                continue
            note_date = created.date() if hasattr(created, "date") else datetime.fromisoformat(str(created).replace("Z", "+00:00")).date()
            if note_date == run_date:
                return True
        return False
    except Exception:
        return False


def _invoice_date(inv: Any) -> Optional[date]:
    """Get invoice date from billed_at, closed_at, updated_at, or created_at (for comparison/sort)."""
    for attr in ("billed_at", "closed_at", "updated_at", "created_at"):
        val = getattr(inv, attr, None)
        if not val:
            continue
        if hasattr(val, "date"):
            return val.date()
        try:
            return datetime.fromisoformat(str(val).replace("Z", "+00:00")).date()
        except Exception:
            continue
    return None


def get_eligible_invoices(
    client: Any,
    subscription_id: str,
    refund_dt_str: Optional[str],
    currency_cd: str = "USD",
) -> List[Any]:
    """
    List subscription invoices, filter to charge + total > 0 + currency + invoice_date >= refund_dt,
    sort by invoice date ascending. Return list of invoice objects.
    """
    refund_dt_str = (refund_dt_str or "").strip()
    if not refund_dt_str:
        return []
    refund_date = _parse_refund_date(refund_dt_str)
    if refund_date is None:
        return []
    out = []
    try:
        pager = client.list_subscription_invoices(subscription_id)
        for inv in pager.items():
            if getattr(inv, "type", None) != "charge":
                continue
            total = getattr(inv, "total", 0) or 0
            if total <= 0:
                continue
            if (getattr(inv, "currency", None) or "").upper() != (currency_cd or "USD").upper():
                continue
            inv_date = _invoice_date(inv)
            if inv_date is None:
                continue
            if inv_date < refund_date:
                continue
            out.append(inv)
    except Exception as e:
        print(f"  Warning: list_subscription_invoices failed: {e}", file=sys.stderr)
        return []
    out.sort(key=lambda inv: _invoice_date(inv) or date.min)
    return out


def process_row(
    client: Any,
    row: Dict[str, Any],
    log_entry: Dict[str, Any],
    dry_run: bool = False,
    run_date: Optional[date] = None,
    row_index: Optional[int] = None,
) -> None:
    """
    Run the 5-step workflow for one CSV row. Mutating API calls skipped when dry_run is True.
    """
    if run_date is None:
        run_date = datetime.now().date()

    account_cd = (row.get("account_cd") or "").strip()
    subscription_guid = (row.get("subscription_guid") or "").strip()
    refund_dt = (row.get("refund_dt") or "").strip()
    account_note = (row.get("account_note") or "").strip()
    currency_cd = (row.get("currency_cd") or "USD").strip() or "USD"

    row_label = f"Row {row_index}" if row_index is not None else "Row"
    print(f"Processing {row_label}: account_cd={account_cd!r}, subscription_guid={subscription_guid!r}, refund_dt={refund_dt!r}")

    log_entry["account_cd"] = account_cd
    log_entry["subscription_guid"] = subscription_guid
    log_entry["subscription_state"] = ""
    log_entry["note_added"] = False
    log_entry["billing_cleared"] = False
    log_entry["refunded_invoice_numbers"] = []
    log_entry["dry_run"] = dry_run
    log_entry["error"] = ""

    sub_id = _subscription_id(subscription_guid)
    acc_id = _account_id(account_cd)
    if not sub_id or not acc_id:
        log_entry["error"] = "missing account_cd or subscription_guid"
        return

    # Step 1: eligible invoices (when refund_dt is set and parseable)
    eligible = get_eligible_invoices(client, sub_id, refund_dt if refund_dt else None, currency_cd)
    parsed = _parse_refund_date(refund_dt) if refund_dt else None
    print(f"  refund_dt={refund_dt!r} -> parsed={parsed}, eligible_invoices={len(eligible)}")
    if refund_dt and parsed is None:
        print(f"  Warning: refund_dt could not be parsed (use m/d/yyyy or yyyy-mm-dd); no refunds will be attempted for this row.")
    refunded_numbers = []

    # Step 2: terminate subscription (no refund; all refunds handled in step 5)
    try:
        if dry_run:
            log_entry["subscription_state"] = "(dry-run) would terminate with refund=none"
        else:
            client.terminate_subscription(sub_id, params={"refund": "none"})
            log_entry["subscription_state"] = "expired"
    except Exception as e:
        err = str(e)
        if "expired" in err.lower() or "canceled" in err.lower() or "not found" in err.lower():
            log_entry["subscription_state"] = "expired_or_invalid"
            # Continue: still try note, billing clear, and refunds if needed
        else:
            log_entry["error"] = err
            # Continue anyway so we can try refunds if eligible

    if not dry_run and not log_entry["subscription_state"]:
        try:
            sub = client.get_subscription(sub_id)
            log_entry["subscription_state"] = getattr(sub, "state", "unknown") or "unknown"
        except Exception:
            log_entry["subscription_state"] = "unknown"

    # Step 3: add account note (skip if account already has a note added today)
    if account_note:
        if dry_run:
            log_entry["note_added"] = True  # would add
        elif _account_has_note_today(client, acc_id, run_date):
            log_entry["note_added"] = False  # already added today, skip
        else:
            try:
                client.create_account_note(acc_id, {"message": account_note})
                log_entry["note_added"] = True
            except Exception as e:
                log_entry["error"] = log_entry["error"] or str(e)

    # Step 4: clear billing info (non-fatal if already cleared)
    if dry_run:
        log_entry["billing_cleared"] = True  # would clear
    else:
        try:
            client.remove_billing_info(acc_id)
            log_entry["billing_cleared"] = True
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                log_entry["billing_cleared"] = False  # already cleared, continue
            else:
                log_entry["error"] = (log_entry["error"] or str(e)) + " (billing)"
            # Continue to step 5 (refunds) even if billing clear failed

    # Step 5: refund all eligible invoices (1 or more)
    if eligible:
        eligible_display = [
            getattr(inv, "number", None) or getattr(inv, "id", None) or "?"
            for inv in eligible
        ]
        print(f"  Eligible invoice numbers: {eligible_display}")
        if dry_run:
            refunded_numbers.extend(
                getattr(inv, "number", None) or getattr(inv, "id", "") for inv in eligible
            )
            print(f"  (dry-run) Would refund {len(eligible)} invoice(s)")
        else:
            for inv in eligible:
                inv_number = getattr(inv, "number", None)
                inv_id = getattr(inv, "id", None)
                api_id = f"number-{inv_number}" if inv_number is not None else (inv_id or "")
                if not api_id:
                    print(f"  Skipping invoice (no number or id): {inv}", file=sys.stderr)
                    continue
                try:
                    # Recurly API expects body with "type"; use request class if available for correct serialization
                    refund_body = _refund_invoice_body()
                    client.refund_invoice(api_id, refund_body)
                    refunded_numbers.append(inv_number or inv_id)
                    print(f"  Refunded invoice: {api_id}")
                except Exception as e:
                    print(f"  Refund failed for invoice {api_id}: {e}", file=sys.stderr)

    log_entry["refunded_invoice_numbers"] = refunded_numbers


def write_log(log_path: Union[str, Path], entries: List[Dict[str, Any]]) -> None:
    """Write log entries to CSV."""
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not entries:
        return
    keys = [
        "account_cd", "subscription_guid", "subscription_state",
        "note_added", "billing_cleared", "refunded_invoice_numbers",
        "dry_run", "error",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for e in entries:
            row = dict(e)
            if "refunded_invoice_numbers" in row and isinstance(row["refunded_invoice_numbers"], list):
                row["refunded_invoice_numbers"] = ";".join(str(x) for x in row["refunded_invoice_numbers"] if x)
            row["note_added"] = "yes" if row.get("note_added") else "no"
            row["billing_cleared"] = "yes" if row.get("billing_cleared") else "no"
            row["dry_run"] = "yes" if row.get("dry_run") else "no"
            w.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Expire Recurly subscriptions, add notes, clear billing, optionally refund (cbscom-sand)."
    )
    parser.add_argument(
        "--input", "-i",
        default=DEFAULT_INPUT_CSV,
        help="Input CSV path (account_cd, subscription_guid, refund_dt, account_note[, currency_cd])",
    )
    parser.add_argument(
        "--log", "-l",
        default=None,
        help="Output log CSV path (default: expire_refund_script_log_yyyymmdd_hhmmss.csv in Downloads)",
    )
    parser.add_argument("--dry-run", "-n", action="store_true", help="Do not call mutating APIs; only log what would be done")
    args = parser.parse_args()

    if args.log is None:
        args.log = _default_log_path()

    if recurly is None:
        print("recurly package not installed. pip install recurly~=4.40", file=sys.stderr)
        return 1

    if RECURLY_API_KEY == "YOUR_API_KEY" or not RECURLY_API_KEY:
        print(
            f"Recurly API key not found. Add key to {RECURLY_KEY_FILE} or set RECURLY_PRIVATE_API_KEY (cbscom-sand.recurly.com).",
            file=sys.stderr,
        )
        return 1

    try:
        rows = load_input(args.input)
    except Exception as e:
        print(f"Load input failed: {e}", file=sys.stderr)
        return 1

    client = recurly.Client(RECURLY_API_KEY)
    log_entries = []
    run_date = datetime.now().date()

    for i, row in enumerate(rows):
        log_entry = {}
        try:
            process_row(
                client,
                row,
                log_entry,
                dry_run=args.dry_run,
                run_date=run_date,
                row_index=i + 1,
            )
        except Exception as e:
            log_entry["account_cd"] = row.get("account_cd", "")
            log_entry["subscription_guid"] = row.get("subscription_guid", "")
            log_entry["subscription_state"] = ""
            log_entry["note_added"] = False
            log_entry["billing_cleared"] = False
            log_entry["refunded_invoice_numbers"] = []
            log_entry["dry_run"] = args.dry_run
            log_entry["error"] = str(e)
        log_entries.append(log_entry)
        if log_entry.get("error"):
            print(f"Row {i + 1} ({log_entry.get('account_cd', '')}): {log_entry['error']}", file=sys.stderr)

    try:
        write_log(args.log, log_entries)
    except Exception as e:
        print(f"Write log failed: {e}", file=sys.stderr)
        return 1

    print(f"Done. Processed {len(rows)} rows; log written to {args.log}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
