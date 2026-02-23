# Backend approach: dashboard data access

How the Payments & Fraud Monitoring Dashboard gets data from BigQuery.

## Chosen approach: direct BigQuery from frontend

- The **frontend** (React app in `dashboard/`) calls **BigQuery** directly to run parameterized queries (or saved queries/views) and display results.
- No thin API layer is required for the initial rollout. This keeps the stack simple and avoids operating an extra service.
- Implementation options for “direct BQ from frontend”:
  - **BigQuery Data API** from the browser: use a backend proxy or **BigQuery Omni / federated auth** only if your org allows it; many environments do not allow direct BQ access from the client for security and billing reasons.
  - **Recommended in practice:** A **small backend proxy** (e.g. Cloud Run or Cloud Functions) that:
    - Accepts filter parameters (date range, `src_system_id`, etc.) from the frontend.
    - Runs the appropriate BigQuery job (saved query or parameterized SQL in `queries/`).
    - Returns JSON to the dashboard.
  - **Alternative:** Run the SQL in `queries/` manually or via a scheduler; export results to a table or file; dashboard reads from that table or file (e.g. via a read-only API or static export).

## Optional: thin API later

If you add a thin API (e.g. Cloud Run/Cloud Functions):

- Expose one endpoint per panel (or one endpoint with a `panel` query param): payments, fraud, subscription, anomaly.
- Request body or query params: same as the **filter contract** ([FILTER_CONTRACT.md](FILTER_CONTRACT.md)) (e.g. `date_start`, `date_end`, `src_system_ids[]`, `gateway_region`, etc.).
- The API runs the corresponding BigQuery job and returns the panel payload as JSON. The frontend already expects the shapes defined in `dashboard/src/types.ts` and used in `dashboard/src/data/mockData.ts`.

No API is implemented in this repo today; the dashboard uses mock data until the chosen access path (direct BQ or proxy) is implemented and wired in Phase 3.
