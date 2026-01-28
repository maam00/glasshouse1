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
echo "[1/7] Scraping Singularity..." >> "$LOG_FILE"
python scripts/scrape_singularity.py >> "$LOG_FILE" 2>&1

# Step 2: Scrape Accountability (Acquisition contracts + products)
echo "[2/7] Scraping Accountability page..." >> "$LOG_FILE"
python scripts/scrape_accountability.py >> "$LOG_FILE" 2>&1

# Step 3: Scrape Careers (Greenhouse API)
echo "[3/7] Scraping Careers data..." >> "$LOG_FILE"
python scripts/scrape_careers.py >> "$LOG_FILE" 2>&1

# Step 4: Merge datasets (uses latest Parcl CSV if available)
echo "[4/7] Merging datasets..." >> "$LOG_FILE"
python scripts/merge_datasets.py >> "$LOG_FILE" 2>&1

# Step 5: Generate dashboard data
echo "[5/7] Generating dashboard data..." >> "$LOG_FILE"
python scripts/generate_unified_dashboard.py >> "$LOG_FILE" 2>&1

# Step 6: Generate AI insights
echo "[6/9] Generating AI insights..." >> "$LOG_FILE"
python scripts/generate_ai_insights.py >> "$LOG_FILE" 2>&1

# Step 7: Generate Problem Homes analysis
echo "[7/9] Generating Problem Homes analysis..." >> "$LOG_FILE"
python scripts/generate_problem_homes.py >> "$LOG_FILE" 2>&1

# Step 8: Generate Market Intelligence Brief
echo "[8/9] Generating Market Intelligence..." >> "$LOG_FILE"
python scripts/generate_market_brief.py >> "$LOG_FILE" 2>&1

# Step 9: Push to GitHub Pages
echo "[9/9] Deploying to GitHub Pages..." >> "$LOG_FILE"
git add outputs/unified_dashboard_data.json outputs/accountability_*.json outputs/careers_*.json outputs/problem_homes_*.json outputs/market_intel_*.json >> "$LOG_FILE" 2>&1
git commit -m "Daily data refresh $(date +%Y-%m-%d)" >> "$LOG_FILE" 2>&1 || true
git push origin master >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "Refresh complete: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

echo "Done! Dashboard data refreshed."
