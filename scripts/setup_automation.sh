#!/bin/bash
# Glass House Automation Setup
# Run this once to set up daily/weekly automated runs

set -e

GLASSHOUSE_DIR="/Users/mabramsky/glasshouse"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

echo "=========================================="
echo "  Glass House Automation Setup"
echo "=========================================="

# Create logs directory
mkdir -p "$GLASSHOUSE_DIR/logs"
echo "✓ Created logs directory"

# Make scripts executable
chmod +x "$GLASSHOUSE_DIR/scripts/daily_run.sh"
chmod +x "$GLASSHOUSE_DIR/scripts/weekly_run.sh"
echo "✓ Made scripts executable"

# Create LaunchAgents directory if needed
mkdir -p "$LAUNCH_AGENTS_DIR"

# Copy plist files to LaunchAgents
cp "$GLASSHOUSE_DIR/com.glasshouse.daily.plist" "$LAUNCH_AGENTS_DIR/"
cp "$GLASSHOUSE_DIR/com.glasshouse.weekly.plist" "$LAUNCH_AGENTS_DIR/"
echo "✓ Copied plist files to LaunchAgents"

# Unload if already loaded (ignore errors)
launchctl unload "$LAUNCH_AGENTS_DIR/com.glasshouse.daily.plist" 2>/dev/null || true
launchctl unload "$LAUNCH_AGENTS_DIR/com.glasshouse.weekly.plist" 2>/dev/null || true

# Load the jobs
launchctl load "$LAUNCH_AGENTS_DIR/com.glasshouse.daily.plist"
launchctl load "$LAUNCH_AGENTS_DIR/com.glasshouse.weekly.plist"
echo "✓ Loaded launchd jobs"

echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Schedule:"
echo "  Daily:  7:00 AM (quick mode, no API credits)"
echo "  Weekly: Monday 7:30 AM (with Parcl market context)"
echo ""
echo "Commands:"
echo "  Check status:    launchctl list | grep glasshouse"
echo "  View daily log:  tail -f $GLASSHOUSE_DIR/logs/launchd_daily.log"
echo "  View weekly log: tail -f $GLASSHOUSE_DIR/logs/launchd_weekly.log"
echo "  Run manually:    $GLASSHOUSE_DIR/scripts/daily_run.sh"
echo ""
echo "To disable:"
echo "  launchctl unload ~/Library/LaunchAgents/com.glasshouse.daily.plist"
echo "  launchctl unload ~/Library/LaunchAgents/com.glasshouse.weekly.plist"
echo ""
