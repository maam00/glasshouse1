#!/bin/bash
# Glass House Daily Run
# Runs the daily intelligence dashboard and logs output

cd /Users/mabramsky/glasshouse

# Activate virtual environment
source venv/bin/activate

# Create log directory
mkdir -p logs

# Run with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/daily_${TIMESTAMP}.log"

echo "========================================" >> "$LOG_FILE"
echo "Glass House Daily Run - $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Run the dashboard (quick mode for daily, no API credits)
python glasshouse.py --quick >> "$LOG_FILE" 2>&1

# Check exit status
if [ $? -eq 0 ]; then
    echo "SUCCESS: Daily run completed" >> "$LOG_FILE"
else
    echo "ERROR: Daily run failed" >> "$LOG_FILE"
fi

# Keep only last 30 days of logs
find logs -name "daily_*.log" -mtime +30 -delete 2>/dev/null

echo "Log saved to: $LOG_FILE"
