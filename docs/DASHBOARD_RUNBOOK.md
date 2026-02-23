# Dashboard runbook: Payments & Fraud Monitoring

How to run the dashboard locally with **live anomaly data from BigQuery**.

## Prerequisites

- **Node.js and npm** (see [dashboard/README.md](../dashboard/README.md)).
- **Python 3** with project venv (same as for `scripts/run_dow_hourly_slack.py`).
- **BigQuery:** Application Default Credentials (e.g. `gcloud auth application-default login` or `scripts/setup-adc.sh`).
- **Project:** `i-dss-streaming-data` (or set `BIGQUERY_PROJECT`).

## Run dashboard with live anomaly data

1. **Install Python deps** (if not already):
   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

2. **Start the API** (serves anomaly data from BigQuery):
   ```bash
   .venv/bin/python api/app.py
   ```
   API runs at **http://localhost:5000**. Optional: `BIGQUERY_PROJECT=i-dss-streaming-data`.

3. **Start the frontend** (in another terminal):
   ```bash
   cd dashboard
   npm install
   npm run dev
   ```
   Dashboard at **http://localhost:5173**.

4. Open **http://localhost:5173**. The **Anomaly** panel will call the API and show BIN deviation data from `i-dss-streaming-data.payment_ops_vw`. Use the global filters (date range, source system); changing them re-fetches from BigQuery.

## If the API is not running

The Anomaly panel falls back to **mock data** and shows a warning. Other panels (Payments, Fraud, Subscription) still use mock data until their backend queries/API are added.

## Data source and queries

- **Anomaly panel:** BigQuery query [queries/dashboard_anomaly_bin_stddev_params.sql](../queries/dashboard_anomaly_bin_stddev_params.sql) (parameterized). Underlying logic: [queries/acct_bin_rate_stddev.sql](../queries/acct_bin_rate_stddev.sql).
- **Dataset:** `i-dss-streaming-data.payment_ops_vw` (views over Recurly; see [DATA_REFRESH.md](DATA_REFRESH.md)).
- **Filter contract:** [FILTER_CONTRACT.md](FILTER_CONTRACT.md). Metric definitions: [METRIC_DEFINITIONS.md](METRIC_DEFINITIONS.md).

## Optional: API base URL

To point the dashboard at a different API (e.g. deployed backend), set when building the frontend:
```bash
VITE_API_BASE_URL=https://your-api.example.com npm run build
```
