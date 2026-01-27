#!/usr/bin/env python3
"""
Glass House - $OPEN Operational Intelligence Platform
======================================================

Main entry point for the tracker.

Usage:
    python glasshouse.py                    # Daily report
    python glasshouse.py --explore          # Explore API structure first
    python glasshouse.py --weekly           # Weekly summary + X post
    python glasshouse.py --backfill 30      # Backfill last 30 days
    python glasshouse.py --sample           # Run with sample data (no API)
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from scripts.daily_pull import main

if __name__ == "__main__":
    sys.exit(main())
