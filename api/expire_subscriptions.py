#!/usr/bin/env python3
"""
Recurly subscription cleanup: expire subscriptions, add account notes,
clear billing info, and optionally refund invoices per CSV input.

Target: https://cbscom-sand.recurly.com/
API key must be for cbscom-sand.recurly.com (set via RECURLY_PRIVATE_API_KEY or below).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Placeholder: set RECURLY_PRIVATE_API_KEY in env, or replace with your API key for cbscom-sand.recurly.com
RECURLY_API_KEY = os.environ.get("RECURLY_PRIVATE_API_KEY", "YOUR_API_KEY")

try:
    import recurly
except ImportError:
    recurly = None


def load_input(csv_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Load CSV with columns account_cd, subscription_guid, refund_dt, account_note (optional: currency_cd)."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return []
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


def get_eligible_invoices(
    client: Any,
    subscription_id: str,
    refund_dt_str: Optional[str],
    currency_cd: str = "USD",
) -> List[Any]:
    """
    List subscription invoices, filter to charge + total > 0 + currency + billed_at >= refund_dt,
    sort by billed_at ascending. Return list of invoice objects.
    """
    if not refund_dt_str or not (refund_dt_str := refund_dt_str.strip()):
        return []
    try:
        refund_date = datetime.strptime(refund_dt_str.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
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
            billed_at = getattr(inv, "billed_at", None) or getattr(inv, "created_at", None)
            if not billed_at:
                continue
            if hasattr(billed_at, "date"):
                inv_date = billed_at.date()
            else:
                try:
                    inv_date = datetime.fromisoformat(str(billed_at).replace("Z", "+00:00")).date()
                except Exception:
                    continue
            if inv_date < refund_date:
                continue
            out.append(inv)
    except Exception:
        return []
    out.sort(key=lambda inv: getattr(inv, "billed_at", None) or getattr(inv, "created_at", None) or "")
    return out


def process_row(
    client: Any, row: Dict[str, Any], log_entry: Dict[str, Any], dry_run: bool = False
) -> None:
    """
    Run the 5-step workflow for one CSV row. Mutating API calls skipped when dry_run is True.
    """
    account_cd = (row.get("account_cd") or "").strip()
    subscription_guid = (row.get("subscription_guid") or "").strip()
    refund_dt = (row.get("refund_dt") or "").strip()
    account_note = (row.get("account_note") or "").strip()
    currency_cd = (row.get("currency_cd") or "USD").strip() or "USD"

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

    # Step 1: eligible invoices (only when refund_dt is set)
    eligible = get_eligible_invoices(client, sub_id, refund_dt or None, currency_cd)
    refunded_numbers = []

    # Step 2: terminate subscription
    try:
        if dry_run:
            if len(eligible) == 1:
                log_entry["subscription_state"] = "(dry-run) would terminate with refund=full"
                refunded_numbers.append(getattr(eligible[0], "number", None) or getattr(eligible[0], "id", ""))
            elif len(eligible) >= 2:
                log_entry["subscription_state"] = "(dry-run) would terminate with refund=none"
            else:
                log_entry["subscription_state"] = "(dry-run) would terminate with refund=none"
        else:
            if len(eligible) == 1:
                client.terminate_subscription(sub_id, params={"refund": "full"})
                refunded_numbers.append(getattr(eligible[0], "number", None) or getattr(eligible[0], "id", ""))
                log_entry["subscription_state"] = "expired"
            else:
                client.terminate_subscription(sub_id, params={"refund": "none"})
                log_entry["subscription_state"] = "expired"
    except Exception as e:
        err = str(e)
        if "expired" in err.lower() or "canceled" in err.lower() or "not found" in err.lower():
            log_entry["subscription_state"] = "expired_or_invalid"
        else:
            log_entry["error"] = err
            return

    if not dry_run and not log_entry["subscription_state"]:
        try:
            sub = client.get_subscription(sub_id)
            log_entry["subscription_state"] = getattr(sub, "state", "unknown") or "unknown"
        except Exception:
            log_entry["subscription_state"] = "unknown"

    # Step 3: add account note
    if account_note:
        if dry_run:
            log_entry["note_added"] = True  # would add
        else:
            try:
                client.create_account_note(acc_id, {"message": account_note})
                log_entry["note_added"] = True
            except Exception as e:
                log_entry["error"] = log_entry["error"] or str(e)

    # Step 4: clear billing info
    if dry_run:
        log_entry["billing_cleared"] = True  # would clear
    else:
        try:
            client.remove_billing_info(acc_id)
            log_entry["billing_cleared"] = True
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                log_entry["billing_cleared"] = False  # none to remove
            else:
                log_entry["error"] = log_entry["error"] or str(e)

    # Step 5: refund 2+ eligible invoices (we already have the list)
    if len(eligible) >= 2 and not dry_run:
        for inv in eligible:
            inv_number = getattr(inv, "number", None)
            inv_id = getattr(inv, "id", None)
            api_id = f"number-{inv_number}" if inv_number is not None else (inv_id or "")
            if not api_id:
                continue
            try:
                client.refund_invoice(api_id, {"type": "full"})
                refunded_numbers.append(inv_number or inv_id)
            except Exception:
                pass
    elif len(eligible) >= 2 and dry_run:
        refunded_numbers.extend(
            getattr(inv, "number", None) or getattr(inv, "id", "") for inv in eligible
        )

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
        default="/Users/gregory.hamilton/Downloads/expire_refund_script_test.csv",
        help="Input CSV path (account_cd, subscription_guid, refund_dt, account_note[, currency_cd])",
    )
    parser.add_argument(
        "--log", "-l",
        default="/Users/gregory.hamilton/Downloads/expire_refund_script_log.csv",
        help="Output log CSV path",
    )
    parser.add_argument("--dry-run", "-n", action="store_true", help="Do not call mutating APIs; only log what would be done")
    args = parser.parse_args()

    if recurly is None:
        print("recurly package not installed. pip install recurly~=4.40", file=sys.stderr)
        return 1

    if RECURLY_API_KEY == "YOUR_API_KEY":
        print("Set RECURLY_PRIVATE_API_KEY or edit RECURLY_API_KEY in script (for cbscom-sand.recurly.com).", file=sys.stderr)
        return 1

    try:
        rows = load_input(args.input)
    except Exception as e:
        print(f"Load input failed: {e}", file=sys.stderr)
        return 1

    client = recurly.Client(RECURLY_API_KEY)
    log_entries = []

    for i, row in enumerate(rows):
        log_entry = {}
        try:
            process_row(client, row, log_entry, dry_run=args.dry_run)
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
