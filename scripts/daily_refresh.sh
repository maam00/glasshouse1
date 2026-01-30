#!/bin/bash
# Daily GlassHouse Data Refresh
# Runs: scraper, data import, tape generator, dashboard data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_DIR/outputs/daily_refresh.log"

cd "$PROJECT_DIR"

echo "========================================" >> "$LOG_FILE"
echo "Daily Refresh: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Activate virtual environment
source venv/bin/activate

# 1. Run daily snapshot scraper (Opendoor listings)
echo "[1/4] Running daily snapshot scraper..." >> "$LOG_FILE"
python3 scripts/daily_snapshot.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Scraper had issues" >> "$LOG_FILE"

# 2. Import any new CSV data from Desktop
echo "[2/4] Importing historical data..." >> "$LOG_FILE"
python3 scripts/import_historical_data.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Import had issues" >> "$LOG_FILE"

# 3. Generate intel tape (jobs + filings)
echo "[3/4] Generating intel tape..." >> "$LOG_FILE"
python3 -m src.tape.tape_generator >> "$LOG_FILE" 2>&1 || echo "  Warning: Tape generator had issues" >> "$LOG_FILE"

# 4. Generate dashboard data
echo "[4/4] Generating dashboard data..." >> "$LOG_FILE"
python3 scripts/generate_dashboard_data.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Dashboard data had issues" >> "$LOG_FILE"

echo "Completed: $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# Print summary
tail -20 "$LOG_FILE"
