#!/usr/bin/env python3
"""
Backfill Historical Data
=========================
Generate historical daily metrics from sales data.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging

from src.db.database import Database
from src.config import KAZ_ERA_START

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# NOTE: KAZ_ERA_START is imported from src.config - single source of truth


def backfill_from_sales(sales_path: str, listings_path: str):
    """Backfill daily metrics from sales data."""

    # Load data
    sales = pd.read_csv(sales_path)
    listings = pd.read_csv(listings_path)

    # Normalize columns
    sales.columns = [c.lower().replace(' ', '_') for c in sales.columns]
    listings.columns = [c.lower().replace(' ', '_') for c in listings.columns]

    # Parse dates
    sales['sale_date'] = pd.to_datetime(sales['sale_date'], errors='coerce')
    sales['purchase_date'] = pd.to_datetime(sales['purchase_date'], errors='coerce')
    listings['purchase_date'] = pd.to_datetime(listings.get('original_purchase_date', listings.get('purchase_date')), errors='coerce')

    # Convert numeric columns in sales
    for col in ['realized_net', 'sale_price', 'purchase_price', 'days_held']:
        if col in sales.columns:
            sales[col] = pd.to_numeric(sales[col].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce').fillna(0)

    # Convert numeric columns in listings
    for col in ['unrealized_net', 'latest_listing_price', 'original_purchase_price', 'days_on_market']:
        if col in listings.columns:
            listings[col] = pd.to_numeric(listings[col].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce').fillna(0)

    # Get date range
    min_date = sales['sale_date'].min().date()
    max_date = sales['sale_date'].max().date()

    logger.info(f"Backfilling from {min_date} to {max_date}")

    db = Database()

    # Process each day
    current_date = min_date
    while current_date <= max_date:
        # Get cumulative sales up to this date
        mask = sales['sale_date'].dt.date <= current_date
        daily_sales = sales[mask].copy()

        if len(daily_sales) == 0:
            current_date += timedelta(days=1)
            continue

        # Calculate metrics for this day
        metrics = calculate_daily_metrics(daily_sales, listings, current_date)

        # Save to database
        db.save_raw_metrics(str(current_date), metrics)
        logger.info(f"  {current_date}: {len(daily_sales)} sales, {metrics['performance']['win_rate']:.1f}% win rate")

        current_date += timedelta(days=1)

    logger.info(f"Backfill complete. {(max_date - min_date).days + 1} days processed.")
    return True


def calculate_daily_metrics(sales: pd.DataFrame, listings: pd.DataFrame, as_of_date) -> dict:
    """Calculate metrics as of a specific date."""

    # Basic performance
    total_sales = len(sales)
    if 'realized_net' in sales.columns:
        profitable = (sales['realized_net'] > 0).sum()
        win_rate = (profitable / total_sales * 100) if total_sales > 0 else 0
        total_profit = sales['realized_net'].sum()
        avg_profit = sales['realized_net'].mean()
    else:
        win_rate = 0
        total_profit = 0
        avg_profit = 0
        profitable = 0

    # Revenue
    revenue = sales['sale_price'].sum() if 'sale_price' in sales.columns else 0

    # Contribution margin
    if 'sale_price' in sales.columns and revenue > 0:
        contribution_margin = (total_profit / revenue * 100)
    else:
        contribution_margin = 0

    # Days held
    if 'days_held' in sales.columns:
        avg_days_held = sales['days_held'].mean()
    else:
        avg_days_held = 0

    # Cohort analysis
    cohorts = calculate_cohorts(sales)

    # Kaz-era metrics
    kaz_era = calculate_kaz_era(sales, listings, as_of_date)

    # Toxic metrics (from listings, simplified for backfill)
    toxic_in_sales = sales[sales['days_held'] >= 365] if 'days_held' in sales.columns else pd.DataFrame()

    return {
        "date": str(as_of_date),
        "performance": {
            "homes_sold_total": total_sales,
            "win_rate": round(win_rate, 1),
            "total_profit": round(total_profit, 0),
            "avg_profit": round(avg_profit, 0),
            "contribution_margin": round(contribution_margin, 2),
            "revenue_total": round(revenue, 0),
        },
        "cohort_new": cohorts.get("new", {}),
        "cohort_mid": cohorts.get("mid", {}),
        "cohort_old": cohorts.get("old", {}),
        "cohort_toxic": cohorts.get("toxic", {}),
        "toxic": {
            "sold_count": len(toxic_in_sales),
            "remaining_count": 84,  # Approximate from current data
            "clearance_pct": round(len(toxic_in_sales) / (len(toxic_in_sales) + 84) * 100, 1) if len(toxic_in_sales) > 0 else 0,
            "sold_avg_loss": round(toxic_in_sales['realized_net'].mean(), 0) if len(toxic_in_sales) > 0 and 'realized_net' in toxic_in_sales.columns else 0,
            "weeks_to_clear": 8,
        },
        "inventory": {
            "total": len(listings),
            "fresh_count": 54,
            "normal_count": 91,
            "stale_count": 164,
            "very_stale_count": 371,
            "toxic_count": 84,
            "avg_dom": 228,
            "legacy_pct": 59.6,
            "total_unrealized_pnl": 58610000,
        },
        "kaz_era": kaz_era,
        "alerts": [],
    }


def calculate_cohorts(sales: pd.DataFrame) -> dict:
    """Calculate cohort metrics."""
    if 'days_held' not in sales.columns or 'realized_net' not in sales.columns:
        return {}

    cohorts = {
        "new": {"min": 0, "max": 90},
        "mid": {"min": 90, "max": 180},
        "old": {"min": 180, "max": 365},
        "toxic": {"min": 365, "max": 9999},
    }

    result = {}
    for name, bounds in cohorts.items():
        mask = (sales['days_held'] >= bounds['min']) & (sales['days_held'] < bounds['max'])
        cohort_sales = sales[mask]

        count = len(cohort_sales)
        if count > 0:
            profitable = (cohort_sales['realized_net'] > 0).sum()
            win_rate = (profitable / count * 100)
            avg_profit = cohort_sales['realized_net'].mean()
            total_revenue = cohort_sales['sale_price'].sum() if 'sale_price' in cohort_sales.columns else 1
            margin = (cohort_sales['realized_net'].sum() / total_revenue * 100) if total_revenue > 0 else 0
        else:
            win_rate = 0
            avg_profit = 0
            margin = 0

        result[name] = {
            "count": count,
            "win_rate": round(win_rate, 1),
            "avg_profit": round(avg_profit, 0),
            "contribution_margin": round(margin, 1),
        }

    return result


def calculate_kaz_era(sales: pd.DataFrame, listings: pd.DataFrame, as_of_date) -> dict:
    """Calculate Kaz-era metrics."""

    # Kaz-era sales (purchased on/after KAZ_ERA_START from config)
    if 'purchase_date' in sales.columns:
        kaz_sales = sales[sales['purchase_date'] >= KAZ_ERA_START]
    else:
        kaz_sales = pd.DataFrame()

    # Kaz-era listings
    if 'purchase_date' in listings.columns:
        kaz_listings = listings[listings['purchase_date'] >= KAZ_ERA_START]
    else:
        kaz_listings = pd.DataFrame()

    # Realized (sold)
    sold_count = len(kaz_sales)
    if sold_count > 0 and 'realized_net' in kaz_sales.columns:
        sold_profitable = (kaz_sales['realized_net'] > 0).sum()
        sold_win_rate = (sold_profitable / sold_count * 100)
        sold_avg_profit = kaz_sales['realized_net'].mean()
    else:
        sold_profitable = 0
        sold_win_rate = 0
        sold_avg_profit = 0

    # Unrealized (on market) - use current listings data
    on_market = len(kaz_listings)
    if on_market > 0:
        # Check for unrealized_net column
        if 'unrealized_net' in kaz_listings.columns:
            above_water = (kaz_listings['unrealized_net'] >= 0).sum()
        elif 'latest_listing_price' in kaz_listings.columns and 'original_purchase_price' in kaz_listings.columns:
            above_water = (kaz_listings['latest_listing_price'] >= kaz_listings['original_purchase_price']).sum()
        else:
            above_water = int(on_market * 0.97)  # Approximate from known data

        above_water_pct = (above_water / on_market * 100)
        underwater = on_market - above_water
    else:
        above_water = 0
        above_water_pct = 0
        underwater = 0

    # Combined
    total = sold_count + on_market
    healthy = sold_profitable + above_water
    health_pct = (healthy / total * 100) if total > 0 else 0

    # Legacy comparison
    if 'purchase_date' in sales.columns:
        legacy_sales = sales[sales['purchase_date'] < KAZ_ERA_START]
        if len(legacy_sales) > 0 and 'realized_net' in legacy_sales.columns:
            legacy_profitable = (legacy_sales['realized_net'] > 0).sum()
            legacy_win_rate = (legacy_profitable / len(legacy_sales) * 100)
        else:
            legacy_win_rate = 64.3  # Approximate
    else:
        legacy_win_rate = 64.3

    improvement = sold_win_rate - legacy_win_rate

    return {
        "realized": {
            "count": sold_count,
            "profitable": int(sold_profitable),
            "win_rate": round(sold_win_rate, 1),
            "avg_profit": round(sold_avg_profit, 0),
        },
        "unrealized": {
            "count": on_market,
            "above_water": int(above_water),
            "above_water_pct": round(above_water_pct, 1),
            "underwater": int(underwater),
        },
        "total": total,
        "overall_health_pct": round(health_pct, 1),
        "vs_legacy_improvement": round(improvement, 1),
    }


if __name__ == "__main__":
    import glob

    # Find CSV files
    csv_dir = Path.home() / "Desktop" / "glasshouse"
    sales_files = list(csv_dir.glob("opendoor-home-sales-*.csv"))
    listings_files = list(csv_dir.glob("opendoor-for-sale-listings-*.csv"))

    if not sales_files or not listings_files:
        logger.error("CSV files not found in ~/Desktop/glasshouse/")
        sys.exit(1)

    sales_path = str(sales_files[0])
    listings_path = str(listings_files[0])

    logger.info(f"Sales: {sales_path}")
    logger.info(f"Listings: {listings_path}")

    backfill_from_sales(sales_path, listings_path)
