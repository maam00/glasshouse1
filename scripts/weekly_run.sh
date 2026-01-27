#!/bin/bash
# Glass House Weekly Run
# Runs full dashboard WITH Parcl market context (~225 credits)
# Schedule: Once per week (e.g., Monday morning)

cd /Users/mabramsky/glasshouse

# Activate virtual environment
source venv/bin/activate

# Create log directory
mkdir -p logs

# Run with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="logs/weekly_${TIMESTAMP}.log"

echo "========================================" >> "$LOG_FILE"
echo "Glass House WEEKLY Run - $(date)" >> "$LOG_FILE"
echo "Includes Parcl market context (~225 credits)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Run with market context (uses ~225 Parcl credits)
python glasshouse.py --market-context >> "$LOG_FILE" 2>&1

# Also run deep analysis
echo "" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
echo "Deep Analysis" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"
python glasshouse.py --deep >> "$LOG_FILE" 2>&1

# Check exit status
if [ $? -eq 0 ]; then
    echo "SUCCESS: Weekly run completed" >> "$LOG_FILE"
else
    echo "ERROR: Weekly run failed" >> "$LOG_FILE"
fi

echo "Log saved to: $LOG_FILE"
