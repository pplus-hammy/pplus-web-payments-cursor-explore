#!/usr/bin/env python3
"""
List invoice numbers for subscriptions from a CSV using the Recurly API.

Reads subscription_guid (and optionally account_cd) from CSV, calls Recurly
list_subscription_invoices for each, and writes subscription_id -> invoice numbers.

API: https://recurly.com/developers/api/v2021-02-25/index.html
- Subscriptions: list_subscription_invoices(subscription_id)
- Target site: https://cbscom-sand.recurly.com/ (API key must be for this site)

Authentication: Recurly expects the API key to be passed as the username in a
Basic Authorization header (password empty). The official recurly-client-python
library handles this when we pass the raw key to recurly.Client(api_key); do
not pre-encode—the client sends Basic base64(api_key + ":").
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

RECURLY_KEY_FILE = "/Users/gregory.hamilton/Desktop/Creds/recurly_us_sbx.txt"
DEFAULT_CSV = "/Users/gregory.hamilton/Downloads/expire_refund_script_test.csv"
DEFAULT_OUTPUT_CSV = "/Users/gregory.hamilton/Downloads/subscription_invoice_info.csv"


def _load_api_key() -> str:
    """
    Load API key from RECURLY_KEY_FILE, then env.
    Key must be the raw value (no prefix); Recurly client sends it as the
    username in Basic auth: Authorization: Basic base64(key + ':').
    """
    path = Path(RECURLY_KEY_FILE)
    if path.exists():
        key = path.read_text(encoding="utf-8").strip()
        if key:
            return key
    return os.environ.get("RECURLY_PRIVATE_API_KEY", "")


RECURLY_API_KEY = _load_api_key()

try:
    import recurly
except ImportError:
    recurly = None


def _subscription_id(value: str) -> str:
    """Return subscription_id for API: add uuid- prefix if value looks like a UUID."""
    s = (value or "").strip()
    if not s:
        return s
    clean = s.replace("-", "")
    if len(clean) == 32 and all(c in "0123456789abcdefABCDEF" for c in clean):
        return f"uuid-{s}" if not s.startswith("uuid-") else s
    return s


def load_subscriptions(csv_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Load CSV with at least subscription_guid; optionally account_cd."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows or "subscription_guid" not in rows[0]:
        raise ValueError("CSV must have column: subscription_guid")
    return rows


def _format_billed_at(billed_at: Any) -> str:
    """Format billed_at for CSV (YYYY-MM-DD)."""
    if billed_at is None:
        return ""
    if hasattr(billed_at, "date"):
        return billed_at.date().isoformat()
    try:
        dt = datetime.fromisoformat(str(billed_at).replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return str(billed_at)[:10] if billed_at else ""


def get_invoice_details_for_subscription(
    client: Any, subscription_id: str
) -> List[Dict[str, Any]]:
    """
    Call Recurly list_subscription_invoices; return list of dicts with
    invoice_number, total, currency, billed_date.
    See: https://recurly.com/developers/api/v2021-02-25/index.html#tag/invoice
    """
    out = []
    try:
        pager = client.list_subscription_invoices(subscription_id)
        for inv in pager.items():
            num = getattr(inv, "number", None)
            inv_number = str(num) if num is not None else (getattr(inv, "id", None) or "")
            total = getattr(inv, "total", None)
            if total is not None and not isinstance(total, str):
                amount = f"{float(total):.2f}"
            else:
                amount = str(total) if total is not None else ""
            currency = (getattr(inv, "currency", None) or "").strip() or ""
            billed_at = getattr(inv, "billed_at", None) or getattr(inv, "created_at", None)
            billed_date = _format_billed_at(billed_at)
            out.append({
                "invoice_number": inv_number,
                "amount": amount,
                "currency_cd": currency,
                "billed_date": billed_date,
            })
    except Exception:
        raise
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List Recurly invoice numbers for subscriptions from a CSV (cbscom-sand)."
    )
    parser.add_argument(
        "--input", "-i",
        default=DEFAULT_CSV,
        help="Input CSV path (must have subscription_guid)",
    )
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_CSV})",
    )
    args = parser.parse_args()

    if recurly is None:
        print("recurly package not installed. pip install recurly~=4.40", file=sys.stderr)
        return 1

    if not RECURLY_API_KEY:
        print(
            f"Recurly API key not found. Set RECURLY_PRIVATE_API_KEY or add key to {RECURLY_KEY_FILE}",
            file=sys.stderr,
        )
        return 1

    try:
        rows = load_subscriptions(args.input)
    except Exception as e:
        print(f"Load input failed: {e}", file=sys.stderr)
        return 1

    # API key as username in Basic auth; client encodes as Authorization: Basic base64(key + ":")
    client = recurly.Client(RECURLY_API_KEY)
    results: List[Dict[str, Any]] = []

    for row in rows:
        subscription_guid = (row.get("subscription_guid") or "").strip()
        account_cd = (row.get("account_cd") or "").strip()
        sub_id = _subscription_id(subscription_guid)
        if not sub_id:
            results.append({
                "account_cd": account_cd,
                "subscription_guid": subscription_guid,
                "invoice_number": "",
                "amount": "",
                "currency_cd": "",
                "billed_date": "",
                "error": "missing subscription_guid",
            })
            continue
        try:
            invoices = get_invoice_details_for_subscription(client, sub_id)
            for inv in invoices:
                results.append({
                    "account_cd": account_cd,
                    "subscription_guid": subscription_guid,
                    "invoice_number": inv.get("invoice_number", ""),
                    "amount": inv.get("amount", ""),
                    "currency_cd": inv.get("currency_cd", ""),
                    "billed_date": inv.get("billed_date", ""),
                    "error": "",
                })
            if not invoices:
                results.append({
                    "account_cd": account_cd,
                    "subscription_guid": subscription_guid,
                    "invoice_number": "",
                    "amount": "",
                    "currency_cd": "",
                    "billed_date": "",
                    "error": "",
                })
        except Exception as e:
            results.append({
                "account_cd": account_cd,
                "subscription_guid": subscription_guid,
                "invoice_number": "",
                "amount": "",
                "currency_cd": "",
                "billed_date": "",
                "error": str(e),
            })
            print(f"Error for {subscription_guid}: {e}", file=sys.stderr)

    fieldnames = [
        "account_cd", "subscription_guid", "invoice_number",
        "amount", "currency_cd", "billed_date", "error",
    ]
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {len(results)} rows to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
