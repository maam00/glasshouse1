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

# Load environment variables (for API keys)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Step 1: Scrape Singularity (FREE - always runs)
echo "[1/6] Scraping Singularity..." >> "$LOG_FILE"
python scripts/scrape_singularity.py >> "$LOG_FILE" 2>&1

# Step 2: Scrape Accountability (Acquisition contracts)
echo "[2/6] Scraping Accountability page..." >> "$LOG_FILE"
python scripts/scrape_accountability.py >> "$LOG_FILE" 2>&1

# Step 3: Merge datasets (uses latest Parcl CSV if available)
echo "[3/6] Merging datasets..." >> "$LOG_FILE"
python scripts/merge_datasets.py >> "$LOG_FILE" 2>&1

# Step 4: Generate dashboard data
echo "[4/6] Generating dashboard data..." >> "$LOG_FILE"
python scripts/generate_unified_dashboard.py >> "$LOG_FILE" 2>&1

# Step 5: Generate AI insights
echo "[5/6] Generating AI insights..." >> "$LOG_FILE"
python scripts/generate_ai_insights.py >> "$LOG_FILE" 2>&1

# Step 6: Push to GitHub Pages
echo "[6/6] Deploying to GitHub Pages..." >> "$LOG_FILE"
git add outputs/unified_dashboard_data.json outputs/accountability_*.json >> "$LOG_FILE" 2>&1
git commit -m "Daily data refresh $(date +%Y-%m-%d)" >> "$LOG_FILE" 2>&1 || true
git push origin master >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "Refresh complete: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

echo "Done! Dashboard data refreshed."
