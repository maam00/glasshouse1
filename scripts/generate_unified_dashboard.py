#!/usr/bin/env python3
"""
Generate dashboard data from unified dataset.

Creates a JSON file optimized for the Glass House dashboard,
using the merged Parcl + Singularity data.

Usage:
    python scripts/generate_unified_dashboard.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

def find_latest_file(directory: Path, pattern: str) -> Path:
    """Find the most recent file matching pattern."""
    files = list(directory.glob(pattern))
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def main():
    project_root = Path(__file__).parent.parent
    output_dir = project_root / "outputs"

    # Load unified data
    sales_file = find_latest_file(output_dir, "unified_sales_*.csv")
    daily_file = find_latest_file(output_dir, "unified_daily_*.csv")
    metrics_file = find_latest_file(output_dir, "unified_metrics_*.json")

    # Also load Singularity map data for geographic info
    map_file = find_latest_file(output_dir, "singularity_map_*.json")

    if not all([sales_file, daily_file, metrics_file]):
        print("Error: Run merge_datasets.py first to create unified data")
        return 1

    print("Loading unified data...")
    sales = pd.read_csv(sales_file)
    daily = pd.read_csv(daily_file)

    with open(metrics_file) as f:
        metrics = json.load(f)

    map_data = []
    if map_file and map_file.exists():
        with open(map_file) as f:
            map_data = json.load(f)

    # Parse dates
    sales['sale_date'] = pd.to_datetime(sales['sale_date'])
    daily['date'] = pd.to_datetime(daily['date'])

    # Filter to Q1 2026
    q1_start = '2026-01-01'
    q1_sales = sales[sales['sale_date'] >= q1_start].copy()
    q1_daily = daily[daily['date'] >= q1_start].copy()

    q1_metrics = metrics['q1_2026']

    print(f"Q1 2026: {len(q1_sales)} sales, {len(q1_daily)} days")

    # =========================================================================
    # BUILD DASHBOARD DATA
    # =========================================================================

    dashboard_data = {
        "generated_at": datetime.now().isoformat(),
        "data_sources": {
            "singularity": "https://singularityresearchfund.com/opendoor-tracker",
            "parcl": "Parcl Research CSV exports",
            "note": "Unified dataset combining both sources"
        },

        # Current date
        "as_of": q1_daily['date'].max().strftime('%Y-%m-%d'),

        # ==== VELOCITY METRICS ====
        "velocity": {
            "q1_sales": int(q1_metrics['sales']['total']),
            "q1_revenue": q1_metrics['revenue']['total'],
            "daily_avg_sales": round(q1_metrics['velocity']['daily_avg'], 1),
            "daily_avg_revenue": q1_metrics['revenue']['daily_avg'],
            "best_day": q1_metrics['velocity']['best_day'],
            "best_day_sales": q1_metrics['velocity']['best_day_count'],
        },

        # ==== P&L METRICS ====
        "pnl": {
            "win_rate": round(q1_metrics['pnl']['win_rate'], 1),
            "avg_profit": round(q1_metrics['pnl']['avg_profit']),
            "avg_loss": round(q1_metrics['pnl']['avg_loss']),
            "total_realized": q1_metrics['pnl']['total_realized'],
            "wins": q1_metrics['pnl']['wins'],
            "losses": q1_metrics['pnl']['losses'],
            "coverage_pct": round(q1_metrics['pnl']['coverage_pct'], 1),
        },

        # ==== REVENUE CHART DATA ====
        "revenue_chart": [],

        # ==== SALES CHART DATA ====
        "sales_chart": [],

        # ==== DATA QUALITY ====
        "data_quality": {
            "total_records": q1_metrics['sales']['total'],
            "matched_records": q1_metrics['sales']['matched'],
            "singularity_only": q1_metrics['sales']['singularity_only'],
            "parcl_only": q1_metrics['sales']['parcl_only'],
            "pnl_coverage": round(q1_metrics['pnl']['coverage_pct'], 1),
            "details_coverage": round(q1_metrics['property_details']['coverage_pct'], 1),
        },
    }

    # Build daily chart data
    for _, row in q1_daily.iterrows():
        date_str = row['date'].strftime('%b %d')

        dashboard_data['revenue_chart'].append({
            "date": date_str,
            "date_full": row['date'].strftime('%Y-%m-%d'),
            "revenue": row['revenue_millions'] * 1_000_000,
            "revenue_millions": round(row['revenue_millions'], 2),
            "sales_count": int(row['sales_count']),
        })

        dashboard_data['sales_chart'].append({
            "date": date_str,
            "date_full": row['date'].strftime('%Y-%m-%d'),
            "count": int(row['sales_count']),
            "moving_avg": round(row['sales_moving_avg'], 1) if pd.notna(row.get('sales_moving_avg')) else None,
        })

    # ==== GEOGRAPHIC DATA ====
    # Aggregate sales by state from unified data
    q1_sales_with_city = q1_sales[q1_sales['city'].notna() & (q1_sales['city'] != '')]

    if len(q1_sales_with_city) > 0:
        # Get state from city (approximate - would need proper geocoding)
        city_counts = q1_sales_with_city['city'].value_counts().head(15)
        dashboard_data['top_cities'] = city_counts.to_dict()

    # Use Singularity map data if available
    if map_data:
        dashboard_data['geographic'] = map_data[:20]  # Top 20 locations

    # ==== COHORT ANALYSIS ====
    # Calculate from sales with days_held data
    sales_with_days = q1_sales[q1_sales['days_held'].notna()].copy()

    if len(sales_with_days) > 0:
        # Define cohorts
        def get_cohort(days):
            if days < 90:
                return 'new'
            elif days < 180:
                return 'mid'
            elif days < 365:
                return 'old'
            else:
                return 'toxic'

        sales_with_days['cohort'] = sales_with_days['days_held'].apply(get_cohort)

        cohorts = {}
        for cohort_name in ['new', 'mid', 'old', 'toxic']:
            cohort_sales = sales_with_days[sales_with_days['cohort'] == cohort_name]
            cohort_with_pnl = cohort_sales[cohort_sales['realized_net'].notna()]

            if len(cohort_with_pnl) > 0:
                wins = len(cohort_with_pnl[cohort_with_pnl['realized_net'] > 0])
                total = len(cohort_with_pnl)
                win_rate = wins / total * 100 if total > 0 else 0
                avg_profit = cohort_with_pnl['realized_net'].mean()
            else:
                win_rate = 0
                avg_profit = 0

            cohorts[cohort_name] = {
                'count': len(cohort_sales),
                'win_rate': round(win_rate, 1),
                'avg_profit': round(avg_profit) if not np.isnan(avg_profit) else 0,
                'total_realized': round(cohort_with_pnl['realized_net'].sum()) if len(cohort_with_pnl) > 0 else 0,
            }

        dashboard_data['cohorts'] = cohorts

    # ==== WEEKLY SUMMARY ====
    # This week's data
    latest_date = q1_daily['date'].max()
    week_start = latest_date - timedelta(days=6)

    this_week = q1_daily[q1_daily['date'] >= week_start]
    last_week_end = week_start - timedelta(days=1)
    last_week_start = last_week_end - timedelta(days=6)
    last_week = q1_daily[(q1_daily['date'] >= last_week_start) & (q1_daily['date'] <= last_week_end)]

    dashboard_data['this_week'] = {
        'sales': int(this_week['sales_count'].sum()),
        'revenue': this_week['revenue_millions'].sum() * 1_000_000,
        'daily_avg': round(this_week['sales_count'].mean(), 1),
    }

    if len(last_week) > 0:
        dashboard_data['last_week'] = {
            'sales': int(last_week['sales_count'].sum()),
            'revenue': last_week['revenue_millions'].sum() * 1_000_000,
            'daily_avg': round(last_week['sales_count'].mean(), 1),
        }

        # Week-over-week change
        wow_sales = (dashboard_data['this_week']['sales'] / dashboard_data['last_week']['sales'] - 1) * 100 if dashboard_data['last_week']['sales'] > 0 else 0
        wow_revenue = (dashboard_data['this_week']['revenue'] / dashboard_data['last_week']['revenue'] - 1) * 100 if dashboard_data['last_week']['revenue'] > 0 else 0

        dashboard_data['wow_change'] = {
            'sales_pct': round(wow_sales, 1),
            'revenue_pct': round(wow_revenue, 1),
        }

    # ==== Q1 GUIDANCE TRACKING ====
    # Opendoor Q1 guidance: ~$1B revenue
    q1_target = 1_000_000_000
    q1_days_total = 90  # Jan 1 - Mar 31
    days_elapsed = len(q1_daily)
    days_remaining = q1_days_total - days_elapsed

    current_revenue = q1_metrics['revenue']['total']
    daily_target = q1_target / q1_days_total
    daily_actual = current_revenue / days_elapsed if days_elapsed > 0 else 0

    projected_revenue = daily_actual * q1_days_total
    pacing_pct = (projected_revenue / q1_target) * 100

    dashboard_data['guidance'] = {
        'q1_target': q1_target,
        'current_revenue': current_revenue,
        'days_elapsed': days_elapsed,
        'days_remaining': days_remaining,
        'daily_target': round(daily_target),
        'daily_actual': round(daily_actual),
        'projected_revenue': round(projected_revenue),
        'pacing_pct': round(pacing_pct, 1),
        'on_track': pacing_pct >= 95,
    }

    # Save
    output_file = output_dir / "unified_dashboard_data.json"
    with open(output_file, 'w') as f:
        json.dump(dashboard_data, f, indent=2, default=str)

    print(f"\nSaved: {output_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("  DASHBOARD DATA SUMMARY")
    print("=" * 60)
    print(f"\n  Q1 2026 ({dashboard_data['as_of']}):")
    print(f"    Sales: {dashboard_data['velocity']['q1_sales']}")
    print(f"    Revenue: ${dashboard_data['velocity']['q1_revenue']:,.0f}")
    print(f"    Daily Avg: {dashboard_data['velocity']['daily_avg_sales']} sales/day")
    print(f"    Win Rate: {dashboard_data['pnl']['win_rate']}%")
    print(f"    Guidance Pacing: {dashboard_data['guidance']['pacing_pct']}%")
    print("=" * 60 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
