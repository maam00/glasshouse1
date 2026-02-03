#!/usr/bin/env python3
"""
Merge Parcl and Singularity datasets into unified Opendoor data.

This script combines:
- Singularity: More complete sales counts, real-time, property details
- Parcl: Financial data (P&L, purchase price, days held)

Usage:
    python scripts/merge_datasets.py
    python scripts/merge_datasets.py --output-dir ./data
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the most recent file matching pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def load_parcl_data(sales_path: Path, listings_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and normalize Parcl CSV data."""
    logger.info(f"Loading Parcl sales: {sales_path}")
    sales = pd.read_csv(sales_path)

    # Normalize column names
    sales = sales.rename(columns={
        'Property ID': 'property_id',
        'Sale Date': 'sale_date_raw',
        'Sale Price': 'sale_price_raw',
        'Purchase Date': 'purchase_date_raw',
        'Purchase Price': 'purchase_price_raw',
        'Days Held': 'days_held',
        'Realized Net': 'realized_net_raw',
        'Quarter': 'quarter',
        'Year': 'year',
        'Buyer Entity': 'buyer_entity',
    })

    # Parse dates
    sales['sale_date'] = pd.to_datetime(sales['sale_date_raw'], format='%b %d, %Y', errors='coerce')
    sales['purchase_date'] = pd.to_datetime(sales['purchase_date_raw'], format='%b %d, %Y', errors='coerce')

    # Parse prices (remove $, commas)
    def parse_price(val):
        if pd.isna(val) or val == '$0' or val == '':
            return np.nan
        return float(str(val).replace('$', '').replace(',', ''))

    sales['sale_price'] = sales['sale_price_raw'].apply(parse_price)
    sales['purchase_price'] = sales['purchase_price_raw'].apply(parse_price)

    # Parse realized net (handle parentheses for negatives)
    def parse_realized(val):
        if pd.isna(val) or val == '' or val == '$0':
            return np.nan
        s = str(val).replace('$', '').replace(',', '')
        if '(' in s:
            s = '-' + s.replace('(', '').replace(')', '')
        return float(s)

    sales['realized_net'] = sales['realized_net_raw'].apply(parse_realized)

    # Add source flag
    sales['source'] = 'parcl'

    logger.info(f"  Loaded {len(sales)} Parcl sales")

    # Load listings
    logger.info(f"Loading Parcl listings: {listings_path}")
    listings = pd.read_csv(listings_path)
    logger.info(f"  Loaded {len(listings)} Parcl listings")

    return sales, listings


def load_singularity_data(sales_path: Path, daily_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load and normalize Singularity data."""
    logger.info(f"Loading Singularity sales: {sales_path}")
    sales = pd.read_csv(sales_path)

    # Normalize column names
    sales = sales.rename(columns={
        'full_address': 'address',
        'list_price': 'sale_price',  # Singularity calls it list_price but it's sale price
        'living_square_footage': 'sqft',
        'sold_date': 'sale_date_raw',
    })

    # Parse dates
    sales['sale_date'] = pd.to_datetime(sales['sale_date_raw'], errors='coerce')

    # Add source flag
    sales['source'] = 'singularity'

    logger.info(f"  Loaded {len(sales)} Singularity sales")

    # Load daily timeseries
    logger.info(f"Loading Singularity daily: {daily_path}")
    daily = pd.read_csv(daily_path)
    daily['date'] = pd.to_datetime(daily['date'])
    logger.info(f"  Loaded {len(daily)} days of timeseries")

    return sales, daily


def match_records(parcl: pd.DataFrame, sing: pd.DataFrame,
                  price_tolerance: float = 0.05) -> pd.DataFrame:
    """
    Match records between Parcl and Singularity by date + price.

    Returns DataFrame with match info.
    """
    logger.info("Matching records between datasets...")

    # Ensure date columns are date objects for comparison
    parcl = parcl.copy()
    sing = sing.copy()
    parcl['match_date'] = parcl['sale_date'].dt.date
    sing['match_date'] = sing['sale_date'].dt.date

    matches = []
    parcl_matched_idx = set()
    sing_matched_idx = set()

    for p_idx, p_row in parcl.iterrows():
        if pd.isna(p_row['sale_price']) or p_row['sale_price'] == 0:
            continue

        p_date = p_row['match_date']
        p_price = p_row['sale_price']

        # Find candidates with same date and similar price
        candidates = sing[
            (sing['match_date'] == p_date) &
            (sing['sale_price'].between(p_price * (1 - price_tolerance),
                                         p_price * (1 + price_tolerance))) &
            (~sing.index.isin(sing_matched_idx))
        ]

        if len(candidates) > 0:
            # Take closest price match
            candidates = candidates.copy()
            candidates['price_diff'] = abs(candidates['sale_price'] - p_price)
            best_match = candidates.loc[candidates['price_diff'].idxmin()]
            s_idx = best_match.name

            parcl_matched_idx.add(p_idx)
            sing_matched_idx.add(s_idx)

            matches.append({
                'parcl_idx': p_idx,
                'sing_idx': s_idx,
                'date': p_date,
                'parcl_price': p_price,
                'sing_price': best_match['sale_price'],
                'price_diff': best_match['price_diff'],
            })

    match_df = pd.DataFrame(matches)
    logger.info(f"  Matched {len(matches)} records")
    logger.info(f"  Parcl unmatched: {len(parcl) - len(parcl_matched_idx)}")
    logger.info(f"  Singularity unmatched: {len(sing) - len(sing_matched_idx)}")

    return match_df, parcl_matched_idx, sing_matched_idx


def create_unified_sales(parcl: pd.DataFrame, sing: pd.DataFrame,
                         matches: pd.DataFrame,
                         parcl_matched: set, sing_matched: set) -> pd.DataFrame:
    """
    Create unified sales dataset combining both sources.

    Priority:
    - For matched records: Merge fields (Singularity details + Parcl financials)
    - For Singularity-only: Use Singularity data (no P&L)
    - For Parcl-only: Use Parcl data (no property details)
    """
    logger.info("Creating unified sales dataset...")

    unified_records = []

    # 1. Process matched records (best of both)
    for _, match in matches.iterrows():
        p_row = parcl.loc[match['parcl_idx']]
        s_row = sing.loc[match['sing_idx']]

        unified_records.append({
            'sale_date': p_row['sale_date'],
            'sale_price': p_row['sale_price'],  # Use Parcl (actual sale price)
            'purchase_price': p_row['purchase_price'],
            'purchase_date': p_row['purchase_date'],
            'days_held': p_row['days_held'],
            'realized_net': p_row['realized_net'],
            'address': s_row.get('address', ''),
            'city': s_row.get('city', ''),
            'beds': s_row.get('beds', np.nan),
            'baths': s_row.get('baths', np.nan),
            'sqft': s_row.get('sqft', np.nan),
            'property_id': p_row.get('property_id', ''),
            'source': 'matched',
            'has_pnl': not pd.isna(p_row['realized_net']),
            'has_details': not pd.isna(s_row.get('beds')),
        })

    # 2. Process Singularity-only records
    for s_idx, s_row in sing.iterrows():
        if s_idx in sing_matched:
            continue

        unified_records.append({
            'sale_date': s_row['sale_date'],
            'sale_price': s_row['sale_price'],
            'purchase_price': np.nan,
            'purchase_date': pd.NaT,
            'days_held': np.nan,
            'realized_net': np.nan,
            'address': s_row.get('address', ''),
            'city': s_row.get('city', ''),
            'beds': s_row.get('beds', np.nan),
            'baths': s_row.get('baths', np.nan),
            'sqft': s_row.get('sqft', np.nan),
            'property_id': '',
            'source': 'singularity_only',
            'has_pnl': False,
            'has_details': not pd.isna(s_row.get('beds')),
        })

    # 3. Process Parcl-only records
    for p_idx, p_row in parcl.iterrows():
        if p_idx in parcl_matched:
            continue

        unified_records.append({
            'sale_date': p_row['sale_date'],
            'sale_price': p_row['sale_price'],
            'purchase_price': p_row['purchase_price'],
            'purchase_date': p_row['purchase_date'],
            'days_held': p_row['days_held'],
            'realized_net': p_row['realized_net'],
            'address': '',
            'city': '',
            'beds': np.nan,
            'baths': np.nan,
            'sqft': np.nan,
            'property_id': p_row.get('property_id', ''),
            'source': 'parcl_only',
            'has_pnl': not pd.isna(p_row['realized_net']),
            'has_details': False,
        })

    unified = pd.DataFrame(unified_records)
    unified = unified.sort_values('sale_date', ascending=False).reset_index(drop=True)

    # Deduplicate by address + sale_date (keep first, which has most data due to sort)
    before_dedup = len(unified)
    unified = unified.drop_duplicates(subset=['address', 'sale_date'], keep='first')
    unified = unified.reset_index(drop=True)
    dedup_removed = before_dedup - len(unified)

    logger.info(f"  Unified dataset: {len(unified)} total sales")
    if dedup_removed > 0:
        logger.info(f"    - Removed {dedup_removed} duplicate records")
    logger.info(f"    - Matched: {len(matches)}")
    logger.info(f"    - Singularity-only: {(unified['source'] == 'singularity_only').sum()}")
    logger.info(f"    - Parcl-only: {(unified['source'] == 'parcl_only').sum()}")

    return unified


def create_unified_daily(sing_daily: pd.DataFrame, unified_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Create unified daily timeseries combining Singularity daily data with
    calculated metrics from unified sales.
    """
    logger.info("Creating unified daily timeseries...")

    # Start with Singularity daily (most complete)
    daily = sing_daily.copy()
    daily = daily.rename(columns={
        'sales_count': 'sales_count_sing',
        'revenue_millions': 'revenue_millions_sing',
    })

    # Calculate daily metrics from unified sales
    unified_sales['date'] = unified_sales['sale_date'].dt.date

    # Daily aggregates
    daily_agg = unified_sales.groupby('date').agg({
        'sale_price': ['count', 'sum', 'mean'],
        'realized_net': ['sum', 'mean', 'count'],
        'has_pnl': 'sum',
    }).reset_index()

    daily_agg.columns = ['date', 'sales_count_unified', 'revenue_unified', 'avg_price',
                          'total_realized', 'avg_realized', 'pnl_count', 'has_pnl_count']
    daily_agg['date'] = pd.to_datetime(daily_agg['date'])
    daily_agg['revenue_millions_unified'] = daily_agg['revenue_unified'] / 1_000_000
    daily_agg['pnl_coverage'] = daily_agg['has_pnl_count'] / daily_agg['sales_count_unified']

    # Merge
    daily = daily.merge(daily_agg, on='date', how='left')

    # Use Singularity counts as primary (more complete)
    daily['sales_count'] = daily['sales_count_sing']
    daily['revenue_millions'] = daily['revenue_millions_sing']

    # Add P&L metrics where available
    daily['daily_realized_net'] = daily['total_realized']
    daily['daily_avg_realized'] = daily['avg_realized']
    daily['pnl_coverage_pct'] = (daily['pnl_coverage'] * 100).round(1)

    logger.info(f"  Daily timeseries: {len(daily)} days")

    return daily


def calculate_summary_metrics(unified_sales: pd.DataFrame,
                              unified_daily: pd.DataFrame,
                              start_date: str = None) -> dict:
    """Calculate summary metrics for dashboard."""

    if start_date:
        sales = unified_sales[unified_sales['sale_date'] >= start_date].copy()
        daily = unified_daily[unified_daily['date'] >= start_date].copy()
    else:
        sales = unified_sales.copy()
        daily = unified_daily.copy()

    # Sales with P&L data
    sales_with_pnl = sales[sales['has_pnl'] == True]

    # Win/loss
    wins = sales_with_pnl[sales_with_pnl['realized_net'] > 0]
    losses = sales_with_pnl[sales_with_pnl['realized_net'] < 0]

    metrics = {
        'period': {
            'start': str(sales['sale_date'].min().date()) if len(sales) > 0 else None,
            'end': str(sales['sale_date'].max().date()) if len(sales) > 0 else None,
            'days': len(daily),
        },
        'sales': {
            'total': len(sales),
            'from_singularity': (sales['source'] != 'parcl_only').sum(),
            'from_parcl': (sales['source'] != 'singularity_only').sum(),
            'matched': (sales['source'] == 'matched').sum(),
            'singularity_only': (sales['source'] == 'singularity_only').sum(),
            'parcl_only': (sales['source'] == 'parcl_only').sum(),
        },
        'revenue': {
            'total': sales['sale_price'].sum(),
            'daily_avg': daily['revenue_millions'].mean() * 1_000_000 if len(daily) > 0 else 0,
            'avg_sale_price': sales['sale_price'].mean(),
        },
        'velocity': {
            'total_sales': len(sales),
            'daily_avg': daily['sales_count'].mean() if len(daily) > 0 else 0,
            'best_day': daily.loc[daily['sales_count'].idxmax()]['date'].strftime('%Y-%m-%d') if len(daily) > 0 else None,
            'best_day_count': int(daily['sales_count'].max()) if len(daily) > 0 else 0,
        },
        'pnl': {
            'coverage_pct': len(sales_with_pnl) / len(sales) * 100 if len(sales) > 0 else 0,
            'total_realized': sales_with_pnl['realized_net'].sum(),
            'win_rate': len(wins) / (len(wins) + len(losses)) * 100 if (len(wins) + len(losses)) > 0 else 0,
            'avg_profit': wins['realized_net'].mean() if len(wins) > 0 else 0,
            'avg_loss': losses['realized_net'].mean() if len(losses) > 0 else 0,
            'wins': len(wins),
            'losses': len(losses),
        },
        'property_details': {
            'coverage_pct': sales['has_details'].sum() / len(sales) * 100 if len(sales) > 0 else 0,
            'avg_beds': sales['beds'].mean(),
            'avg_baths': sales['baths'].mean(),
            'avg_sqft': sales['sqft'].mean(),
        },
        'data_quality': {
            'singularity_pct': (sales['source'] != 'parcl_only').sum() / len(sales) * 100 if len(sales) > 0 else 0,
            'parcl_pct': (sales['source'] != 'singularity_only').sum() / len(sales) * 100 if len(sales) > 0 else 0,
            'full_data_pct': (sales['source'] == 'matched').sum() / len(sales) * 100 if len(sales) > 0 else 0,
        },
    }

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="Merge Parcl and Singularity Opendoor datasets"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=None,
        help="Output directory (default: ./outputs)"
    )
    parser.add_argument(
        "--parcl-sales",
        default=None,
        help="Path to Parcl sales CSV"
    )
    parser.add_argument(
        "--parcl-listings",
        default=None,
        help="Path to Parcl listings CSV"
    )
    parser.add_argument(
        "--sing-sales",
        default=None,
        help="Path to Singularity sales CSV"
    )
    parser.add_argument(
        "--sing-daily",
        default=None,
        help="Path to Singularity daily CSV"
    )
    args = parser.parse_args()

    # Determine paths
    project_root = Path(__file__).parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else project_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find Parcl files
    parcl_dir = Path.home() / "Desktop" / "glasshouse"
    parcl_sales_path = args.parcl_sales or find_latest_file(parcl_dir, "*sales*.csv")
    parcl_listings_path = args.parcl_listings or find_latest_file(parcl_dir, "*listing*.csv")

    # Find Singularity files
    sing_sales_path = args.sing_sales or find_latest_file(output_dir, "singularity_sales_*.csv")
    sing_daily_path = args.sing_daily or find_latest_file(output_dir, "singularity_daily_*.csv")

    # Validate files exist
    missing = []
    if not parcl_sales_path or not parcl_sales_path.exists():
        missing.append("Parcl sales CSV")
    if not parcl_listings_path or not parcl_listings_path.exists():
        missing.append("Parcl listings CSV")
    if not sing_sales_path or not sing_sales_path.exists():
        missing.append("Singularity sales CSV (run scrape_singularity.py first)")
    if not sing_daily_path or not sing_daily_path.exists():
        missing.append("Singularity daily CSV (run scrape_singularity.py first)")

    if missing:
        logger.error("Missing required files:")
        for m in missing:
            logger.error(f"  - {m}")
        return 1

    print("\n" + "=" * 70)
    print("  MERGING OPENDOOR DATASETS")
    print("=" * 70)

    # Load data
    parcl_sales, parcl_listings = load_parcl_data(parcl_sales_path, parcl_listings_path)
    sing_sales, sing_daily = load_singularity_data(sing_sales_path, sing_daily_path)

    # Match records
    matches, parcl_matched, sing_matched = match_records(parcl_sales, sing_sales)

    # Create unified datasets
    unified_sales = create_unified_sales(parcl_sales, sing_sales, matches, parcl_matched, sing_matched)
    unified_daily = create_unified_daily(sing_daily, unified_sales)

    # Calculate metrics
    q1_metrics = calculate_summary_metrics(unified_sales, unified_daily, start_date='2026-01-01')
    all_metrics = calculate_summary_metrics(unified_sales, unified_daily)

    # Save outputs
    timestamp = datetime.now().strftime("%Y-%m-%d")

    # Unified sales
    sales_file = output_dir / f"unified_sales_{timestamp}.csv"
    unified_sales.to_csv(sales_file, index=False)
    logger.info(f"Saved: {sales_file}")

    # Unified daily
    daily_file = output_dir / f"unified_daily_{timestamp}.csv"
    unified_daily.to_csv(daily_file, index=False)
    logger.info(f"Saved: {daily_file}")

    # Metrics JSON
    metrics_file = output_dir / f"unified_metrics_{timestamp}.json"
    with open(metrics_file, 'w') as f:
        json.dump({'q1_2026': q1_metrics, 'all_time': all_metrics}, f, indent=2, default=str)
    logger.info(f"Saved: {metrics_file}")

    # Also save match info for debugging
    if len(matches) > 0:
        matches_file = output_dir / f"dataset_matches_{timestamp}.csv"
        matches.to_csv(matches_file, index=False)

    # Print summary
    print("\n" + "=" * 70)
    print("  UNIFIED DATASET SUMMARY")
    print("=" * 70)

    print(f"\n  Total Sales: {q1_metrics['sales']['total']}")
    print(f"    - Matched (both sources): {q1_metrics['sales']['matched']}")
    print(f"    - Singularity-only: {q1_metrics['sales']['singularity_only']}")
    print(f"    - Parcl-only: {q1_metrics['sales']['parcl_only']}")

    print(f"\n  Data Coverage:")
    print(f"    - P&L data: {q1_metrics['pnl']['coverage_pct']:.1f}% of sales")
    print(f"    - Property details: {q1_metrics['property_details']['coverage_pct']:.1f}% of sales")
    print(f"    - Full data (both): {q1_metrics['data_quality']['full_data_pct']:.1f}% of sales")

    print(f"\n  Q1 2026 Metrics:")
    print(f"    - Total Revenue: ${q1_metrics['revenue']['total']:,.0f}")
    print(f"    - Daily Avg Sales: {q1_metrics['velocity']['daily_avg']:.1f} homes")
    print(f"    - Win Rate: {q1_metrics['pnl']['win_rate']:.1f}% (from {q1_metrics['pnl']['wins'] + q1_metrics['pnl']['losses']} sales with P&L)")
    print(f"    - Avg Profit: ${q1_metrics['pnl']['avg_profit']:,.0f}")
    print(f"    - Total Realized: ${q1_metrics['pnl']['total_realized']:,.0f}")

    print("\n" + "=" * 70)
    print(f"  Output files saved to: {output_dir}")
    print("=" * 70 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
