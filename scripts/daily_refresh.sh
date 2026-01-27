#!/bin/bash
# =============================================================================
# Glass House - Daily Data Refresh
# =============================================================================
# Run this daily via cron to keep dashboard updated
#
# Cron example (run at 6 PM daily):
#   0 18 * * * /Users/mabramsky/glasshouse/scripts/daily_refresh.sh
#
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$PROJECT_DIR/logs/refresh_$(date +%Y%m%d).log"

# Create logs directory
mkdir -p "$PROJECT_DIR/logs"

echo "========================================" >> "$LOG_FILE"
echo "Glass House Refresh: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

cd "$PROJECT_DIR"

# Activate virtual environment
source venv/bin/activate

# Step 1: Scrape Singularity (FREE - always runs)
echo "[1/3] Scraping Singularity..." >> "$LOG_FILE"
python scripts/scrape_singularity.py >> "$LOG_FILE" 2>&1

# Step 2: Merge datasets (uses latest Parcl CSV if available)
echo "[2/3] Merging datasets..." >> "$LOG_FILE"
python scripts/merge_datasets.py >> "$LOG_FILE" 2>&1

# Step 3: Generate dashboard data
echo "[3/3] Generating dashboard data..." >> "$LOG_FILE"
python scripts/generate_unified_dashboard.py >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "Refresh complete: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Optional: Copy to web server location
# cp "$PROJECT_DIR/outputs/unified_dashboard_data.json" /var/www/html/data/

echo "Done! Dashboard data refreshed."
