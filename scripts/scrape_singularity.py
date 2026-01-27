#!/usr/bin/env python3
"""
Scrape Opendoor data from Singularity Research Fund tracker.
https://singularityresearchfund.com/opendoor-tracker

This scraper fetches:
1. Sales history (all individual sales with dates, prices, locations)
2. Daily chart data (sales counts and revenue by day)
3. Map data (geographic distribution)

Usage:
    python scripts/scrape_singularity.py
    python scripts/scrape_singularity.py --output-dir ./data
"""

import sys
import json
import re
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://singularityresearchfund.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": f"{BASE_URL}/unlock-opendoor",
    "Accept": "application/json, text/html, */*",
}


def fetch_sales_history(limit_per_page: int = 100, max_records: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch all sales history from the API.

    Args:
        limit_per_page: Number of records per API call
        max_records: Maximum total records to fetch (None = all)

    Returns:
        DataFrame with all sales records
    """
    logger.info("Fetching sales history...")

    all_sales = []
    offset = 0

    while True:
        url = f"{BASE_URL}/api/opendoor/sales-history?offset={offset}&limit={limit_per_page}"

        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            sales = response.json()

            if not sales:
                logger.info(f"  No more records at offset {offset}")
                break

            all_sales.extend(sales)
            logger.info(f"  Fetched {len(sales)} records (total: {len(all_sales)})")

            offset += limit_per_page

            if max_records and len(all_sales) >= max_records:
                all_sales = all_sales[:max_records]
                break

        except requests.RequestException as e:
            logger.error(f"  Error fetching sales: {e}")
            break

    if not all_sales:
        return pd.DataFrame()

    df = pd.DataFrame(all_sales)

    # Parse dates
    if 'sold_date' in df.columns:
        df['sold_date'] = pd.to_datetime(df['sold_date']).dt.date

    logger.info(f"Total sales fetched: {len(df)}")

    if not df.empty and 'sold_date' in df.columns:
        logger.info(f"Date range: {df['sold_date'].min()} to {df['sold_date'].max()}")

    return df


def fetch_dashboard_data() -> dict:
    """
    Fetch the dashboard page and extract embedded chart data.

    Returns:
        Dict with 'daily_sales' and 'daily_revenue' chart data
    """
    logger.info("Fetching dashboard page for chart data...")

    url = f"{BASE_URL}/unlock-opendoor"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text

        # Extract chart data using regex
        # Pattern: const chartData = {...}
        chart_pattern = r'const chartData = ({[^;]+});'
        matches = re.findall(chart_pattern, html)

        result = {}

        if len(matches) >= 1:
            # First chart is daily sales count
            try:
                daily_sales = json.loads(matches[0])
                result['daily_sales'] = daily_sales
                logger.info(f"  Found daily sales chart: {len(daily_sales.get('labels', []))} days")
            except json.JSONDecodeError:
                logger.warning("  Could not parse daily sales chart data")

        if len(matches) >= 2:
            # Second chart is daily revenue
            try:
                daily_revenue = json.loads(matches[1])
                result['daily_revenue'] = daily_revenue
                logger.info(f"  Found daily revenue chart: {len(daily_revenue.get('labels', []))} days")
            except json.JSONDecodeError:
                logger.warning("  Could not parse daily revenue chart data")

        return result

    except requests.RequestException as e:
        logger.error(f"  Error fetching dashboard: {e}")
        return {}


def fetch_map_data() -> list:
    """
    Fetch geographic map data from the API.

    Returns:
        List of city-level data
    """
    logger.info("Fetching map data...")

    url = f"{BASE_URL}/api/opendoor/map-data"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"  Fetched map data for {len(data)} locations")
        return data
    except requests.RequestException as e:
        logger.error(f"  Error fetching map data: {e}")
        return []


def build_daily_timeseries(chart_data: dict) -> pd.DataFrame:
    """
    Convert chart data to a proper timeseries DataFrame.

    Args:
        chart_data: Dict with 'daily_sales' and 'daily_revenue' keys

    Returns:
        DataFrame with date index and sales/revenue columns
    """
    if not chart_data:
        return pd.DataFrame()

    # Get the labels (dates) from daily sales
    daily_sales = chart_data.get('daily_sales', {})
    daily_revenue = chart_data.get('daily_revenue', {})

    labels = daily_sales.get('labels', [])

    if not labels:
        return pd.DataFrame()

    # Parse dates - format is "Jan 26" etc
    # Need to figure out the year based on month
    current_year = datetime.now().year
    dates = []

    for label in labels:
        try:
            # Parse "Jan 26" format
            dt = datetime.strptime(f"{label} {current_year}", "%b %d %Y")
            # If month is > current month, it's probably last year
            if dt.month > datetime.now().month + 1:
                dt = dt.replace(year=current_year - 1)
            dates.append(dt.date())
        except ValueError:
            dates.append(None)

    df = pd.DataFrame({
        'date': dates,
        'sales_count': daily_sales.get('data', []),
        'sales_moving_avg': daily_sales.get('moving_avg', []),
        'revenue_millions': daily_revenue.get('data', []),
        'revenue_moving_avg': daily_revenue.get('moving_avg', []),
        'is_ceo_transition': daily_sales.get('is_ceo_transition', []),
    })

    df = df.dropna(subset=['date'])
    df = df.sort_values('date')

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Opendoor data from Singularity Research Fund"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=None,
        help="Output directory (default: ./outputs)"
    )
    parser.add_argument(
        "--max-sales",
        type=int,
        default=None,
        help="Maximum number of sales records to fetch"
    )
    parser.add_argument(
        "--sales-only",
        action="store_true",
        help="Only fetch sales history"
    )
    args = parser.parse_args()

    # Determine output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(__file__).parent.parent / "outputs"

    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d")

    print("\n" + "=" * 60)
    print("  Singularity Research Fund - Opendoor Tracker Scraper")
    print("=" * 60)

    # 1. Fetch sales history
    sales_df = fetch_sales_history(max_records=args.max_sales)

    if not sales_df.empty:
        sales_file = output_dir / f"singularity_sales_{timestamp}.csv"
        sales_df.to_csv(sales_file, index=False)
        print(f"\nSaved {len(sales_df)} sales to: {sales_file}")

    if args.sales_only:
        print("\n" + "=" * 60 + "\n")
        return 0

    # 2. Fetch dashboard chart data
    chart_data = fetch_dashboard_data()

    if chart_data:
        # Build timeseries
        timeseries_df = build_daily_timeseries(chart_data)

        if not timeseries_df.empty:
            ts_file = output_dir / f"singularity_daily_{timestamp}.csv"
            timeseries_df.to_csv(ts_file, index=False)
            print(f"Saved {len(timeseries_df)} days of data to: {ts_file}")

        # Save raw chart data
        chart_file = output_dir / f"singularity_charts_{timestamp}.json"
        with open(chart_file, 'w') as f:
            json.dump(chart_data, f, indent=2)
        print(f"Saved chart data to: {chart_file}")

    # 3. Fetch map data
    map_data = fetch_map_data()

    if map_data:
        map_file = output_dir / f"singularity_map_{timestamp}.json"
        with open(map_file, 'w') as f:
            json.dump(map_data, f, indent=2)
        print(f"Saved map data ({len(map_data)} locations) to: {map_file}")

    # Summary
    print("\n" + "-" * 60)
    print("Summary:")
    print(f"  Sales records: {len(sales_df)}")
    if not sales_df.empty and 'sold_date' in sales_df.columns:
        print(f"  Date range: {sales_df['sold_date'].min()} to {sales_df['sold_date'].max()}")
    if chart_data:
        daily_sales = chart_data.get('daily_sales', {})
        if daily_sales.get('labels'):
            print(f"  Chart data: {daily_sales['labels'][0]} to {daily_sales['labels'][-1]}")
    print("=" * 60 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
