---
name: Gateway comparison analysis
overview: Build a comprehensive gateway A vs C comparison covering direct financials (success rates, costs, refunds, chargebacks), customer lifetime value impact, and what-if routing scenarios. Deliver both a console+Excel analysis of the provided data and a reusable Python script.
todos:
  - id: build-script
    content: Write the full Python script in scripts/gateway_compare.py with data loading, calculations, console output, Excel output, and chart generation
    status: completed
  - id: run-analysis
    content: Run the script against gateway_performance_test.xlsx to produce the comparison output, charts, and verify results
    status: completed
  - id: review-output
    content: Review output for correctness, present methodology explanation and final comparison to the user
    status: completed
isProject: false
---

# Gateway A vs C Performance Comparison

## Data from Excel

**Parameters:**

- Gateway A cost: 3.8% of successful transaction revenue
- Gateway C cost: 2.7% of successful transaction revenue
- Average transaction amount: $10.76
- Average customer lifetime: 16 months (1 CI + 15 renewals)

**Monthly data (6 months, Oct-25 through Mar-26):** success rates, volumes, refund $/txn, and chargeback $/txn for CI and Renewal on each gateway.

---

## Methodology

### Section 1: Monthly Direct Financial Comparison

For each month, for each gateway, calculate:

- **Volume and Success:** CI/Renewal attempts, success rates, successful and failed transaction counts
- **Gross Revenue:** `successful_txns * $10.76`
- **Gateway Processing Cost:** `gross_revenue * gateway_cost_rate`
- **Refunds:** `successful_CI * refund_per_CI + successful_Ren * refund_per_Ren`
- **Chargebacks:** `successful_CI * CB_per_CI + successful_Ren * CB_per_Ren`
- **Net Revenue:** `gross - gateway_cost - refunds - chargebacks`
- **Net Revenue per Attempt** and **per Successful Transaction** (efficiency metrics)

Side-by-side Gateway A vs C with difference column (A - C).

### Section 2: Per-Transaction Expected Value (Normalized)

Removes volume differences to compare gateway efficiency head-to-head:

- `Expected Net per CI attempt = SR_CI * ($10.76 - $10.76 * cost_rate - refund_per_CI - CB_per_CI)`
- `Expected Net per Renewal attempt = SR_Ren * ($10.76 - $10.76 * cost_rate - refund_per_Ren - CB_per_Ren)`

### Section 3: Customer Lifetime Value (CLV) Impact

A failed CI transaction = a permanently lost customer. Each lost customer forfeits:

- **Immediate:** $10.76 (the failed signup)
- **Future renewals:** 15 months * $10.76 = $161.40
- **Total CLV lost per failed CI:** 16 * $10.76 = $172.16

Failed renewals lose only the immediate $10.76 (customer remains active).

Per month: `total_economic_impact = failed_CI * $172.16 + failed_Ren * $10.76`

### Section 4: What-If Analysis

"If ALL combined volume were routed through one gateway, what would the outcome be?"

Apply each gateway's observed success rates to the combined CI and Renewal volume, then compute net revenue, refunds, chargebacks, and CLV impact.

### Section 5: 6-Month Totals

Aggregate all sections across the full Oct-25 through Mar-26 period with weighted average success rates and cumulative financials.

---

## Output

- **Console:** Formatted tables (similar to existing `gateway_compare.py` style) with monthly breakdowns and period totals
- **Excel:** A new workbook with sheets for each section, formatted with headers, borders, and number formatting
- **Charts (PNG):** Presentation-ready static images saved alongside the output Excel file, using matplotlib with a clean style

### Charts to Generate

1. **Success Rates by Month** - Grouped bar chart with 4 series (CI-A, CI-C, Ren-A, Ren-C) across 6 months. Highlights where each gateway wins on approval rates for each transaction type.
2. **Monthly Net Revenue Comparison** - Side-by-side bars (Gateway A vs C) showing net revenue after all deductions (gateway cost, refunds, chargebacks) per month, with a line overlay for cumulative totals.
3. **CLV Impact of Failed CI Transactions** - Stacked bar chart per month showing immediate lost revenue vs future renewal revenue lost, for each gateway. Demonstrates the outsized economic cost of CI failures.
4. **Cost Breakdown Waterfall** - 6-month aggregate waterfall chart for each gateway: starts at gross revenue, steps down through gateway costs, refunds, chargebacks, ending at net revenue. Makes it easy to see where money goes.
5. **What-If Scenario Comparison** - Horizontal bar chart comparing total 6-month net revenue and total CLV impact under three scenarios: actual split, all-via-A, all-via-C. Answers the routing decision question at a glance.

All charts will use a consistent color scheme (e.g., blue for Gateway A, orange for Gateway C), include clear titles, axis labels, data labels where helpful, and be sized for 16:9 slide decks.

---

## Python Script ([scripts/gateway_compare.py](scripts/gateway_compare.py))

Replace the existing inline heredoc script with a proper Python module that:

1. **Reads** the xlsx file using `openpyxl` (handles the strict OOXML format by parsing sheet XML directly if needed, since this file uses strict conformance which openpyxl struggles with)
2. **Parses** parameters (rows 1-4) and monthly data table (rows 6-12), mapping shared strings and date values
3. **Calculates** all 5 sections of metrics
4. **Prints** formatted console report
5. **Writes** results to a new Excel workbook
6. **Generates** 5 presentation-ready PNG charts using matplotlib

The script will accept the input xlsx path as a command-line argument (defaulting to the provided file location) and write the output xlsx and chart PNGs to an `output/` directory alongside it.

Key functions:

- `load_data(filepath)` - parse xlsx, return params dict and list of monthly data dicts
- `calculate_monthly(month, params)` - compute all metrics for one month
- `aggregate_totals(months)` - weighted totals across period
- `what_if(months, params)` - combined-volume scenarios
- `print_report(results)` - console output
- `write_excel(results, output_path)` - Excel output
- `generate_charts(results, output_dir)` - create all 5 PNG charts
- `main()` - CLI entry point

