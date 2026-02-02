#!/bin/bash
# Glass House Daily Run
# Runs at 12:00 PM daily (after manual Parcl data drop at 9am)
# Full pipeline: Scrape → Merge → Generate dashboard data → Deploy

set -e

cd /Users/mabramsky/glasshouse1

# Activate virtual environment
source venv/bin/activate

# Create log directory
mkdir -p logs

# Run with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TODAY=$(date +%Y-%m-%d)
LOG_FILE="logs/daily_${TIMESTAMP}.log"

echo "========================================" | tee -a "$LOG_FILE"
echo "Glass House Daily Run - $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Step 1: Scrape Singularity for sales data (FREE - always works)
echo "[1/7] Scraping Singularity tracker..." | tee -a "$LOG_FILE"
python scripts/scrape_singularity.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Singularity scrape failed" >> "$LOG_FILE"

# Step 2: Merge datasets (Parcl CSVs from Desktop + Singularity)
echo "[2/7] Merging datasets..." | tee -a "$LOG_FILE"
python scripts/merge_datasets.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Merge failed" >> "$LOG_FILE"

# Step 3: Generate unified dashboard data
echo "[3/7] Generating unified dashboard data..." | tee -a "$LOG_FILE"
python scripts/generate_unified_dashboard.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Dashboard gen failed" >> "$LOG_FILE"

# Step 4: Scrape Accountability page (acquisition contracts)
echo "[4/7] Scraping Accountability page..." | tee -a "$LOG_FILE"
python scripts/scrape_accountability.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Accountability scrape failed" >> "$LOG_FILE"

# Step 5: Scrape Careers data (Greenhouse API)
echo "[5/7] Scraping Careers data..." | tee -a "$LOG_FILE"
python scripts/scrape_careers.py >> "$LOG_FILE" 2>&1 || echo "  Warning: Careers scrape failed" >> "$LOG_FILE"

# Step 6: Generate AI insights (if Anthropic API key available)
echo "[6/7] Generating AI insights..." | tee -a "$LOG_FILE"
python scripts/generate_ai_insights.py >> "$LOG_FILE" 2>&1 || echo "  Warning: AI insights failed (API key may be missing)" >> "$LOG_FILE"

# Step 7: Deploy to GitHub Pages
echo "[7/7] Deploying to GitHub Pages..." | tee -a "$LOG_FILE"
git add outputs/*.json outputs/*.csv >> "$LOG_FILE" 2>&1 || true
git commit -m "Daily data refresh ${TODAY}" >> "$LOG_FILE" 2>&1 || echo "  No changes to commit" >> "$LOG_FILE"
git push origin master >> "$LOG_FILE" 2>&1 || echo "  Warning: Git push failed" >> "$LOG_FILE"

echo "========================================" | tee -a "$LOG_FILE"
echo "SUCCESS: Daily run completed at $(date)" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"

# Keep only last 30 days of logs
find logs -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true
