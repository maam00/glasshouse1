#!/bin/bash
# Glass House Daily Run
# Runs at 12:00 PM daily (after manual Parcl data drop at 9am)
# Full pipeline: Import CSVs → Scrape → Generate data → AI insights → Market brief

cd /Users/mabramsky/glasshouse

# Activate virtual environment
source venv/bin/activate

# Create log directory
mkdir -p logs

# Run with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TODAY=$(date +%Y-%m-%d)
LOG_FILE="logs/daily_${TIMESTAMP}.log"

echo "========================================" >> "$LOG_FILE"
echo "Glass House Daily Run - $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Step 1: Import latest Parcl CSVs from Desktop
echo "[1/6] Importing Parcl data from Desktop..." >> "$LOG_FILE"
DESKTOP_DIR="$HOME/Desktop/glasshouse"
if [ -d "$DESKTOP_DIR" ]; then
    # Find today's files (or most recent)
    SALES_CSV=$(ls -t "$DESKTOP_DIR"/opendoor-home-sales-*.csv 2>/dev/null | head -1)
    LISTINGS_CSV=$(ls -t "$DESKTOP_DIR"/opendoor-for-sale-listings-*.csv 2>/dev/null | head -1)

    if [ -n "$SALES_CSV" ] && [ -n "$LISTINGS_CSV" ]; then
        cp "$SALES_CSV" "data/imports/sales_latest.csv"
        cp "$LISTINGS_CSV" "data/imports/listings_latest.csv"
        echo "  Imported: $(basename "$SALES_CSV")" >> "$LOG_FILE"
        echo "  Imported: $(basename "$LISTINGS_CSV")" >> "$LOG_FILE"
    else
        echo "  WARNING: No Parcl CSVs found in $DESKTOP_DIR" >> "$LOG_FILE"
    fi
else
    echo "  WARNING: Desktop folder not found: $DESKTOP_DIR" >> "$LOG_FILE"
fi

# Step 2: Run main dashboard data generation
echo "[2/6] Running dashboard data generation..." >> "$LOG_FILE"
python glasshouse.py --quick >> "$LOG_FILE" 2>&1

# Step 3: Scrape Singularity for additional data
echo "[3/6] Scraping Singularity tracker..." >> "$LOG_FILE"
python scripts/scrape_singularity.py >> "$LOG_FILE" 2>&1

# Step 4: Generate unified dashboard data
echo "[4/6] Generating unified dashboard data..." >> "$LOG_FILE"
python scripts/generate_unified_dashboard.py >> "$LOG_FILE" 2>&1

# Step 5: Generate AI insights (Claude API)
echo "[5/6] Generating AI insights..." >> "$LOG_FILE"
python scripts/generate_ai_insights.py >> "$LOG_FILE" 2>&1

# Step 6: Generate market brief + news bullets (Claude API)
echo "[6/6] Generating market brief..." >> "$LOG_FILE"
python scripts/generate_market_brief.py >> "$LOG_FILE" 2>&1

# Check final status
if [ $? -eq 0 ]; then
    echo "========================================" >> "$LOG_FILE"
    echo "SUCCESS: Daily run completed at $(date)" >> "$LOG_FILE"
else
    echo "========================================" >> "$LOG_FILE"
    echo "ERROR: Daily run failed at $(date)" >> "$LOG_FILE"
fi

# Keep only last 30 days of logs
find logs -name "daily_*.log" -mtime +30 -delete 2>/dev/null

echo "Log saved to: $LOG_FILE"
