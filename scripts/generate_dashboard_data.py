#!/usr/bin/env python3
"""
Generate Dashboard Data
========================
Export data from database + intelligence files to JSON for web dashboard.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import glob
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

    # Get all historical data from database
    today = datetime.now().strftime("%Y-%m-%d")
    all_metrics = db.get_metrics_range("2026-01-01", today)

    if not all_metrics:
        logger.warning("No metrics found in database")
        return None

    # Sort by date
    all_metrics.sort(key=lambda x: x.get("date", ""))

    # Current = most recent from database
    current = all_metrics[-1] if all_metrics else {}

    # Enrich current with latest intelligence file (has advanced analytics)
    intelligence_files = sorted(glob.glob(str(output_dir / "intelligence_*.json")))
    if intelligence_files:
        latest_intel = intelligence_files[-1]
        try:
            with open(latest_intel) as f:
                intel_data = json.load(f)

            # Merge advanced analytics into current
            if "advanced" in intel_data:
                adv = intel_data["advanced"]
                current["kaz_era"] = adv.get("kaz_era", {})
                current["guidance"] = adv.get("guidance", {})
                current["velocity"] = adv.get("velocity", {})
                current["pricing"] = adv.get("pricing", {})
                current["risk"] = adv.get("risk", {})
                current["markets"] = adv.get("markets", [])

            # Merge market data
            if "market" in intel_data:
                mkt = intel_data["market"]
                current["stock_price"] = mkt.get("stock", {}).get("price", 5.87)
                current["stock_change"] = mkt.get("stock", {}).get("pct_from_52_high", -46)

            logger.info(f"  Merged from: {latest_intel}")

        except Exception as e:
            logger.warning(f"Could not read intelligence file: {e}")

    # Fallback stock price
    if "stock_price" not in current:
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
    logger.info(f"  Has kaz_era: {'kaz_era' in current}")
    logger.info(f"  Has guidance: {'guidance' in current}")

    return output_file


if __name__ == "__main__":
    generate_dashboard_data()
