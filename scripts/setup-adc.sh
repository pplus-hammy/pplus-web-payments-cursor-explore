#!/usr/bin/env bash
# Step 2: Configure Application Default Credentials for BigQuery MCP.
# Run in your own terminal (not from Cursor's automated run) so the browser can open.
# Usage: ./scripts/setup-adc.sh

set -e
PROJECT="${BIGQUERY_PROJECT:-i-dss-streaming-data}"

echo "Setting default project to $PROJECT..."
gcloud config set project "$PROJECT"

echo "Logging in (Application Default Credentials). A browser window will open..."
gcloud auth application-default login

echo ""
echo "Verifying ADC..."
if gcloud auth application-default print-access-token >/dev/null 2>&1; then
  echo "OK: Application Default Credentials are set. You can proceed to step 3."
else
  echo "ADC check failed. Ensure you completed the browser sign-in."
  exit 1
fi
