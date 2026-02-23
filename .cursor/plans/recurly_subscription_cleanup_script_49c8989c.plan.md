---
name: Recurly subscription cleanup script
overview: Add a Python script that uses the Recurly API (via recurly-client-python) to terminate subscriptions, add account notes, clear billing info, and optionally refund invoices based on a CSV input, writing results to a log file. The script targets https://cbscom-sand.recurly.com/ with placeholder API credentials.
todos: []
isProject: false
---

# Recurly subscription cleanup script

## API reference summary

Based on [Recurly API v2021-02-25](https://recurly.com/developers/api/v2021-02-25/index.html) and the [recurly-client-python](https://recurly-client-python.readthedocs.io/en/stable) docs:


| Action                          | Client method                                                                    | Notes                                                                                                                                                                     |
| ------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Expire subscription immediately | `terminate_subscription(subscription_id, params={"refund": "full"})` or `"none"` | Use `refund="full"` when there is **exactly one** >$0 eligible invoice (refunds that invoice). Otherwise use `refund="none"` and refund via `refund_invoice` per invoice. |
| Add account note                | `create_account_note(account_id, body)`                                          | `body = {"message": "<account_note>"}`. Account ID: use `code-{account_cd}` when using account code.                                                                      |
| Clear billing info              | `remove_billing_info(account_id)`                                                | Single billing info per account (non-Wallet). For Wallet, `remove_a_billing_info(account_id, billing_info_id)` may be needed.                                             |
| List subscription invoices      | `list_subscription_invoices(subscription_id)`                                    | Returns a Pager; no server-side filter by amount/date—filter in Python. Call **before** terminate when refund_dt is set, to decide refund path.                           |
| Refund invoice                  | `refund_invoice(invoice_id, body)`                                               | `body = {"type": "full"}` for full refund. Only used when there are **2+** eligible invoices; for exactly one, use `terminate_subscription(..., refund="full")` instead.  |


- **Subscription ID**: Support both ID and UUID; if CSV has UUID, pass with `uuid-` prefix (e.g. `uuid-123457890`).
- **Site**: The API key is associated with a Recurly site (subdomain). Using the key for **cbscom-sand** will target `https://cbscom-sand.recurly.com/`. No extra base-URL config in the client—credentials placeholder only.

---

## Implementation plan

### 1. Dependencies and config

- Add `**recurly`** (e.g. `recurly~=4.40` per [Recurly Python](https://recurly-client-python.readthedocs.io/en/stable)) to [requirements.txt](requirements.txt). Keep existing deps (e.g. `pandas` for CSV).
- In the script (or a small config section):
  - **Placeholder for API key**: e.g. read from env `RECURLY_PRIVATE_API_KEY` or a constant `RECURLY_API_KEY = "YOUR_API_KEY"` with a comment that the key must be for **cbscom-sand.recurly.com**.
  - Initialize client: `recurly.Client(api_key)` (and `region="eu"` only if the site is EU).

### 2. CSV input

- **Path**: CLI argument or constant (e.g. `--input subscriptions.csv`).
- **Columns**: `account_cd`, `subscription_guid`, `refund_dt`, `account_note`.
  - `refund_dt`: optional date; if **populated** → refund eligible invoices back to this date (inclusive); if **blank** → no refunds.
  - Parse dates consistently (e.g. `YYYY-MM-DD`); treat blank/empty as “no refund”.

### 3. Per-row workflow (order of operations)

For each CSV row:

1. **Eligible invoices (only when `refund_dt` is set)**
  - Call `list_subscription_invoices(subscription_guid)` and iterate (e.g. `.items()`).  
  - **Filter**: `type == "charge"`, `total > 0`, `currency == "USD"`, and `billed_at >= refund_dt`.  
  - **Sort**: by billed date (e.g. ascending). Collect the list `eligible_invoices`.  
  - If `refund_dt` is blank, set `eligible_invoices = []`.
2. **Terminate subscription**
  - If there is **exactly one** eligible invoice: call `terminate_subscription(subscription_guid, params={"refund": "full"})` so Recurly refunds that invoice as part of the terminate. Log that single invoice number as refunded.
  - Otherwise (0 or 2+ eligible invoices): call `terminate_subscription(subscription_guid, params={"refund": "none"})`.  
  - If subscription is already expired/canceled, handle idempotently (e.g. catch and log, then continue).
3. **Add account note**
  - If `account_note` is non-empty: `create_account_note(f"code-{account_cd}", {"message": account_note})`.  
  - Log whether the note was added (success/failure).
4. **Clear billing information**
  - Call `remove_billing_info(f"code-{account_cd}")`.  
  - If the account has no billing info or uses Wallet (multiple billing infos), handle 404 or use `list_account_billing_infos` + `remove_a_billing_info` as needed; log whether billing was cleared.
5. **Refunds (only if `refund_dt` is set)**
  - Call `list_subscription_invoices(subscription_guid)` and iterate (e.g. `.items()`).
  - **Filter**: `type == "charge"`, `total > 0`, `currency == "USD"`, and `billed_at >= refund_dt` (interpret “refund back to date” as: refund invoices billed on or after `refund_dt`).  
  - **Sort**: by invoice billed date (e.g. ascending) so order is well-defined.  
  - For each eligible invoice: `refund_invoice(invoice.id or number, {"type": "full"})`; collect invoice numbers (or ids) for the log.  
  - If an invoice is already refunded/closed, skip or catch error and log.

Use **account code** with prefix `code-{account_cd}` for all account-scoped calls; **subscription** with `uuid-{subscription_guid}` if the CSV contains UUIDs.

### 4. Log file

- **Path**: CLI argument or derived from input (e.g. `--log run_20250223.log` or `subscriptions_log_<timestamp>.csv`).
- **Format**: One record per CSV row. Suggested columns:
  - `account_cd`
  - `subscription_guid`
  - `subscription_state` (e.g. `active` / `expired` / `canceled`) — from subscription object after terminate or from get before/after.
  - `note_added` (boolean or yes/no)
  - `billing_cleared` (boolean or yes/no)
  - `refunded_invoice_numbers` (array/list of invoice numbers refunded; empty if no refund_dt or none eligible)

Prefer a structured format (e.g. CSV or JSON lines) so it’s easy to parse later.

### 5. Error handling and robustness

- **Try/except per row**: One failing row (e.g. invalid subscription, already refunded invoice) should not stop the whole run; log the error and continue.
- **Idempotency**: Terminate is effectively idempotent for already-expired subscriptions; note/billing/refund should handle “already done” (e.g. 404, 422) and log accordingly.
- **Logging**: Log to stdout/stderr and append to the log file (e.g. which rows succeeded/failed and any API error messages).

### 6. Script structure (suggested)

- **Entrypoint**: `python expire_subscriptions.py --input <csv> --log <log_file>` (or equivalent).
- **Functions**: e.g. `load_input(csv_path)`, `process_row(client, row, log_entry)`, `get_eligible_invoices(client, subscription_id, refund_dt)`, `write_log(log_path, entries)`.
- **Single responsibility**: Read CSV → for each row run the 4 steps above → collect log entries → write log file.

---

## Clarification: “refund back to” date

- **Interpretation**: Refund invoices whose **billed date** is **on or after** `refund_dt` (i.e. from that date forward). Filter: `billed_at >= refund_dt`, then sort by `billed_at` ascending.
- If you instead need “refund all invoices **up to** that date” (billed_at <= refund_dt), the filter can be switched to that in implementation.

---

## File to add

- **New script**: e.g. `api/expire_subscriptions.py` (or `scripts/expire_subscriptions.py`) containing the logic above, with placeholder for API credentials and connection to `https://cbscom-sand.recurly.com/` via the Recurly client and the chosen credentials.

---

## Optional enhancements (out of scope unless requested)

- Dry-run mode (no API writes, only log what would be done).
- Retries with backoff for transient API errors.
- Support for non-USD or multi-currency filtering if needed later.

