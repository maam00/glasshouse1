#!/usr/bin/env python3
"""
Generate Dashboard Data
========================
Export data from database to JSON for web dashboard.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from datetime import datetime
from src.db.database import Database
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


def generate_dashboard_data(output_dir: Path = None) -> Path:
    """Generate JSON data for web dashboard."""
    output_dir = output_dir or Path(__file__).parent.parent / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    db = Database()

    # Get all historical data
    today = datetime.now().strftime("%Y-%m-%d")
    all_metrics = db.get_metrics_range("2026-01-01", today)

    if not all_metrics:
        logger.warning("No metrics found in database")
        return None

    # Sort by date
    all_metrics.sort(key=lambda x: x.get("date", ""))

    # Current = most recent
    current = all_metrics[-1] if all_metrics else {}

    # Add stock price (would come from external data in production)
    current["stock_price"] = 5.87

    # Build dashboard data
    dashboard_data = {
        "generated_at": datetime.now().isoformat(),
        "current": current,
        "history": all_metrics,
    }

    # Save to file
    output_file = output_dir / "dashboard_data.json"
    with open(output_file, "w") as f:
        json.dump(dashboard_data, f, indent=2, default=str)

    logger.info(f"Dashboard data saved: {output_file}")
    logger.info(f"  Current date: {current.get('date')}")
    logger.info(f"  History: {len(all_metrics)} days")

    return output_file


if __name__ == "__main__":
    generate_dashboard_data()
